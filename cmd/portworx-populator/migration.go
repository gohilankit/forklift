package main

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Config holds the configuration for the migration.
type Config struct {
	// FlashArray settings
	FAIP       string
	FAAPIVer   string
	FAAPIToken string

	// Worker settings
	Jobs          int // XCOPY goroutines for step 4
	Step2Workers  int // PXD poke workers for step 2
	StatsInterval int // Stats interval in seconds (0 = disable)
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() Config {
	return Config{
		FAAPIVer:      DefaultFAVersion,
		Jobs:          DefaultStep4Workers,
		Step2Workers:  DefaultStep2Workers,
		StatsInterval: DefaultStatsInterval,
	}
}

// Constants
const (
	DefaultSegmentLength = 107374182400 // 100 GiB segments
	DefaultFAVersion     = "2.41"
	DefaultBlockSize     = 4096 // 4K block_size for FA diff
	DefaultSegmentOffset = 0
	DefaultThinBlockSize = 64 * 1024 // 64 KiB
	DefaultPokeSize      = 512       // bytes
	DefaultStep2Workers  = 16
	DefaultStep4Workers  = 512
	DefaultStatsInterval = 30
	MaxXcopyWorkersCap   = 4096
)

// /dev/null reuse for sg_xcopy
var (
	devNull     *os.File
	devNullOnce sync.Once
)

func getDevNull() *os.File {
	devNullOnce.Do(func() {
		f, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
		if err != nil {
			panic(fmt.Errorf("failed to open %s: %w", os.DevNull, err))
		}
		devNull = f
	})
	return devNull
}

// LogFunc is a function type for logging.
type LogFunc func(format string, args ...interface{})

// defaultLogger logs with timestamp to stdout.
func defaultLogger(format string, args ...interface{}) {
	ts := time.Now().Format("2006-01-02 15:04:05")
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("%s | %s\n", ts, msg)
}

// RunMigration executes the full 4-step migration pipeline.
// It migrates data from source PX volume (FADA) to destination PX volume (PXD) using below 4 steps:
//  1. Fetch FlashArray diff extents
//  2. Prepare PXD thin mappings (poke extents)
//  3. Build FA->backend mapping
//  4. Copy data via XCOPY
func RunMigration(srcPxVol, dstPxVol string, cfg Config) error {
	return RunMigrationWithLogger(srcPxVol, dstPxVol, cfg, defaultLogger)
}

// RunMigrationWithLogger executes the migration with a custom logger.
func RunMigrationWithLogger(srcPxVol, dstPxVol string, cfg Config, log LogFunc) error {
	startAll := time.Now()
	log("Starting FADA->PX backend migration pipeline")
	log("Source FADA PX volume: %s", srcPxVol)
	log("Destination PXD PX volume: %s", dstPxVol)

	shortToken := cfg.FAAPIToken
	if len(shortToken) > 6 {
		shortToken = shortToken[:6] + "..."
	}
	log("Using FlashArray: ip=%s, api_ver=%s (token: %s)", cfg.FAIP, cfg.FAAPIVer, shortToken)

	// Create migrator instance
	m := &migrator{
		cfg: cfg,
		log: log,
	}

	// STEP 1: Fetch FA diff extents
	if err := m.runStep1FetchExtents(srcPxVol); err != nil {
		log("ERROR: Step 1 failed: %v", err)
		return fmt.Errorf("step 1 failed: %w", err)
	}

	// STEP 2: Prepare PXD thin mappings
	if err := m.runStep2PreparePxdMappings(srcPxVol, dstPxVol); err != nil {
		log("ERROR: Step 2 failed: %v", err)
		return fmt.Errorf("step 2 failed: %w", err)
	}

	// Wait for thin pool metadata to be committed before Step 3
	log("Waiting 2 seconds for thin pool metadata to be committed...")
	time.Sleep(2 * time.Second)

	// STEP 3: Build FA->backend mapping
	mappingFile, aggFile, err := m.runStep3BuildMapping(srcPxVol, dstPxVol)
	if err != nil {
		log("ERROR: Step 3 failed: %v", err)
		return fmt.Errorf("step 3 failed: %w", err)
	}
	log("Step 3: mapping files: detailed=%s, aggregated=%s", mappingFile, aggFile)

	// STEP 4: Copy data via XCOPY
	if err := m.runStep4CopyData(srcPxVol, dstPxVol); err != nil {
		log("ERROR: Step 4 failed: %v", err)
		return fmt.Errorf("step 4 failed: %w", err)
	}

	elapsedAll := time.Since(startAll).Seconds()
	log("All steps completed. Total elapsed: %.1f seconds", elapsedAll)
	return nil
}

// migrator holds the state for a migration run.
type migrator struct {
	cfg Config
	log LogFunc
}

// -----------------------------
// Host execution helpers
// -----------------------------

// isInContainer checks if we're running in a container with host access
func isInContainer() bool {
	selfNs, err := os.Readlink("/proc/self/ns/mnt")
	if err != nil {
		return false
	}
	pid1Ns, err := os.Readlink("/proc/1/ns/mnt")
	if err != nil {
		return false
	}
	return selfNs != pid1Ns
}

// needsNsenterForDevices checks if we need nsenter for device operations
func needsNsenterForDevices() bool {
	if !isInContainer() {
		return false
	}
	if _, err := os.Stat("/dev/pxd"); err == nil {
		return false
	}
	return true
}

// wrapWithChrootHost wraps command with chroot /host
func wrapWithChrootHost(args []string) []string {
	return append([]string{"chroot", "/host"}, args...)
}

// wrapWithNsenter prepends nsenter command if running in container
func wrapWithNsenter(args []string) []string {
	if !isInContainer() {
		return args
	}
	nsenterArgs := []string{
		"nsenter",
		"--target", "1",
		"--mount",
		"--uts",
		"--ipc",
		"--net",
		"--pid",
		"--",
	}
	return append(nsenterArgs, args...)
}

// runCmd executes a command, wrapping with nsenter if in container
func runCmd(args ...string) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("runCmd: empty command")
	}
	wrappedArgs := wrapWithNsenter(args)
	cmd := exec.Command(wrappedArgs[0], wrappedArgs[1:]...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("command failed (%v): %s\nOutput: %s", err, strings.Join(args, " "), string(out))
	}
	return string(out), nil
}

// -----------------------------
// Common parsing helpers
// -----------------------------

// parseSize: parse "4096", "0x1000", "64K", "4M", "2G"
func parseSize(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size string")
	}
	mult := uint64(1)
	last := s[len(s)-1]
	if last == 'k' || last == 'K' || last == 'm' || last == 'M' || last == 'g' || last == 'G' {
		switch last {
		case 'k', 'K':
			mult = 1024
		case 'm', 'M':
			mult = 1024 * 1024
		case 'g', 'G':
			mult = 1024 * 1024 * 1024
		}
		s = s[:len(s)-1]
	}
	base := 10
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		base = 16
	}
	v, err := strconv.ParseUint(s, base, 64)
	if err != nil {
		return 0, err
	}
	return v * mult, nil
}

// Extent represents a data extent with offset and length.
type Extent struct {
	Offset uint64
	Length uint64
}

func loadExtents(path string) ([]Extent, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var extents []Extent
	sc := bufio.NewScanner(f)
	lineno := 0
	for sc.Scan() {
		lineno++
		line := sc.Text()
		line = strings.SplitN(line, "#", 2)[0]
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid extent at line %d: %q", lineno, line)
		}
		off, err := parseSize(parts[0])
		if err != nil {
			return nil, fmt.Errorf("line %d: offset parse error: %v", lineno, err)
		}
		length, err := parseSize(parts[1])
		if err != nil {
			return nil, fmt.Errorf("line %d: length parse error: %v", lineno, err)
		}
		extents = append(extents, Extent{Offset: off, Length: length})
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return extents, nil
}

// -----------------------------
// PX helpers
// -----------------------------

// PxVolInfo holds information about a Portworx volume.
type PxVolInfo struct {
	Name    string
	VolID   string
	DevPath string
}

func getPxVolInfoWithDevice(volArg string) (*PxVolInfo, error) {
	out, err := runCmd("pxctl", "v", "i", volArg)
	if err != nil {
		return nil, fmt.Errorf("failed to run 'pxctl v i %s': %w", volArg, err)
	}
	var name, volID, devPath string
	sc := bufio.NewScanner(strings.NewReader(out))
	for sc.Scan() {
		line := sc.Text()
		if !strings.Contains(line, ":") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(parts[0]))
		val := strings.TrimSpace(parts[1])
		switch key {
		case "name":
			name = val
		case "volume":
			volID = val
		case "device path":
			devPath = val
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if volID == "" {
		return nil, fmt.Errorf("could not parse Volume ID from pxctl output for %s", volArg)
	}
	if name == "" {
		name = volArg
	}
	return &PxVolInfo{Name: name, VolID: volID, DevPath: devPath}, nil
}

func getPxClusterIDPrefix() (string, error) {
	out, err := runCmd("pxctl", "cluster", "list")
	if err != nil {
		return "", fmt.Errorf("pxctl cluster list failed: %w", err)
	}
	sc := bufio.NewScanner(strings.NewReader(out))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if strings.HasPrefix(line, "Cluster UUID") {
			fields := strings.Fields(line)
			if len(fields) >= 3 {
				uuid := fields[2]
				if idx := strings.Index(uuid, "-"); idx > 0 {
					uuid = uuid[:idx]
				}
				return uuid, nil
			}
		}
	}
	if err := sc.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("could not find Cluster UUID from pxctl output")
}

// -----------------------------
// Step 1: FA diff extents
// -----------------------------

type faDiffItem struct {
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
}

type faDiffResponse struct {
	Items []faDiffItem `json:"items"`
}

func faLogin(ip, apiVer, apiToken string) (string, error) {
	url := fmt.Sprintf("https://%s/api/%s/login", ip, apiVer)
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	client := &http.Client{Transport: tr, Timeout: 30 * time.Second}
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("api-token", apiToken)
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("FA login failed: status=%d", resp.StatusCode)
	}
	xAuth := resp.Header.Get("X-Auth-Token")
	if xAuth == "" {
		return "", fmt.Errorf("FA login: missing X-Auth-Token header")
	}
	return xAuth, nil
}

func faFetchDiffExtents(ip, apiVer, xAuthToken, faVolName string, segLen, blockSize, segOff uint64) ([]faDiffItem, error) {
	url := fmt.Sprintf("https://%s/api/%s/volumes/diff?names=%s&segment_length=%d&block_size=%d&segment_offset=%d",
		ip, apiVer, faVolName, segLen, blockSize, segOff)

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	client := &http.Client{Transport: tr, Timeout: 120 * time.Second}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Auth-Token", xAuthToken)
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("FA diff extents failed: status=%d body=%s", resp.StatusCode, string(body))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var diff faDiffResponse
	if err := json.Unmarshal(body, &diff); err != nil {
		return nil, fmt.Errorf("unmarshal diff JSON: %w", err)
	}
	if diff.Items == nil {
		diff.Items = []faDiffItem{}
	}
	return diff.Items, nil
}

func (m *migrator) runStep1FetchExtents(srcPxVol string) error {
	m.log("===== STEP 1/4: Fetch FlashArray diff extents =====")
	m.log("Step 1: determining FlashArray volume name for source PX volume %q", srcPxVol)

	clusterID, err := getPxClusterIDPrefix()
	if err != nil {
		return fmt.Errorf("step1: failed to get PX cluster ID: %w", err)
	}
	faVolName := fmt.Sprintf("px_%s-%s", clusterID, srcPxVol)
	m.log("Step 1: using FlashArray volume name %q", faVolName)

	m.log("Step 1: logging in to FlashArray %s (API %s)", m.cfg.FAIP, m.cfg.FAAPIVer)
	xAuth, err := faLogin(m.cfg.FAIP, m.cfg.FAAPIVer, m.cfg.FAAPIToken)
	if err != nil {
		return fmt.Errorf("step1: FA login failed: %w", err)
	}
	m.log("Step 1: FlashArray login succeeded")

	m.log("Step 1: fetching diff extents (segment_length=%d, block_size=%d, segment_offset=%d)",
		DefaultSegmentLength, DefaultBlockSize, DefaultSegmentOffset)

	items, err := faFetchDiffExtents(m.cfg.FAIP, m.cfg.FAAPIVer, xAuth, faVolName, DefaultSegmentLength, DefaultBlockSize, DefaultSegmentOffset)
	if err != nil {
		return fmt.Errorf("step1: fetch diff extents failed: %w", err)
	}

	var totalBytes uint64
	var minLen, maxLen uint64
	for _, it := range items {
		totalBytes += it.Length
		if minLen == 0 || it.Length < minLen {
			minLen = it.Length
		}
		if it.Length > maxLen {
			maxLen = it.Length
		}
	}
	giB := float64(totalBytes) / (1024.0 * 1024.0 * 1024.0)
	m.log("Step 1: received %d extents from FlashArray (total bytes=%d, ~%.2f GiB)", len(items), totalBytes, giB)

	extFile := fmt.Sprintf("%s.extents", srcPxVol)
	f, err := os.Create(extFile)
	if err != nil {
		return fmt.Errorf("step1: create extents file: %w", err)
	}
	defer f.Close()

	fmt.Fprintf(f, "# Offset  Length (bytes)\n")
	for _, it := range items {
		fmt.Fprintf(f, "%d %d\n", it.Offset, it.Length)
	}
	m.log("Step 1: extents written to %s", extFile)
	m.log("Step 1: summary: extents=%d, segment_length=%d, block_size=%d", len(items), DefaultSegmentLength, DefaultBlockSize)
	return nil
}

// -----------------------------
// Step 2: Poke PXD thin mapping
// -----------------------------

func pokeExtentRangeWithBuf(f *os.File, offset, length, thinBlockSize uint64, buf []byte) (uint64, error) {
	if length == 0 {
		return 0, nil
	}
	origOffset := offset
	start := origOffset & ^(thinBlockSize - 1)
	end := origOffset + length - 1

	var blocks uint64
	for pos := start; pos <= end; pos += thinBlockSize {
		if _, err := f.WriteAt(buf, int64(pos)); err != nil {
			return blocks, fmt.Errorf("write at offset %d failed: %w", pos, err)
		}
		blocks++
	}
	return blocks, nil
}

func (m *migrator) runStep2PreparePxdMappings(srcPxVol, dstPxVol string) error {
	m.log("===== STEP 2/4: Prepare PXD thin mappings (poke extents) =====")
	m.log("Step 2: preparing thin mappings on destination PX volume %q", dstPxVol)

	dstInfo, err := getPxVolInfoWithDevice(dstPxVol)
	if err != nil {
		return fmt.Errorf("step2: getPxVolInfo: %w", err)
	}
	if dstInfo.DevPath == "" {
		return fmt.Errorf("step2: device path not found for volume %s", dstPxVol)
	}

	extFile := fmt.Sprintf("%s.extents", srcPxVol)
	extents, err := loadExtents(extFile)
	if err != nil {
		return fmt.Errorf("step2: load extents from %s: %w", extFile, err)
	}
	if len(extents) == 0 {
		m.log("Step 2: no extents found in %s, nothing to poke", extFile)
		return nil
	}

	var totalBytes uint64
	for _, e := range extents {
		totalBytes += e.Length
	}
	giB := float64(totalBytes) / (1024.0 * 1024.0 * 1024.0)
	approxBlocks := totalBytes / DefaultThinBlockSize

	m.log("Step 2: device=%s, extents=%d, total_bytes=%d (~%.2f GiB), approx_thin_blocks=%d",
		dstInfo.DevPath, len(extents), totalBytes, giB, approxBlocks)

	workers := m.cfg.Step2Workers
	if workers <= 0 {
		workers = DefaultStep2Workers
	}
	if workers > len(extents) {
		workers = len(extents)
	}
	if workers < 1 {
		workers = 1
	}

	thinKB := DefaultThinBlockSize / 1024
	m.log("Step 2: thin_block_size=%d KiB, poke_size=%d bytes, workers=%d",
		thinKB, DefaultPokeSize, workers)

	tasks := make(chan Extent, len(extents))
	for _, e := range extents {
		tasks <- e
	}
	close(tasks)

	var wg sync.WaitGroup
	var extentsDone uint64
	var blocksPoked uint64

	start := time.Now()

	// stats goroutine
	if m.cfg.StatsInterval > 0 {
		ticker := time.NewTicker(time.Duration(m.cfg.StatsInterval) * time.Second)
		go func() {
			defer ticker.Stop()
			var prevBlocks uint64
			for range ticker.C {
				ed := atomic.LoadUint64(&extentsDone)
				bp := atomic.LoadUint64(&blocksPoked)
				if int(ed) >= len(extents) {
					return
				}
				elapsed := time.Since(start).Seconds()
				deltaBlk := bp - prevBlocks
				prevBlocks = bp

				m.log("Step 2: progress: %d/%d extents, blocks_poked=%d (+%d), elapsed=%.1fs",
					ed, len(extents), bp, deltaBlk, elapsed)
			}
		}()
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fd, err := os.OpenFile(dstInfo.DevPath, os.O_RDWR, 0)
			if err != nil {
				m.log("Step 2: ERROR: open %s failed: %v", dstInfo.DevPath, err)
				return
			}
			defer func() {
				if err := fd.Sync(); err != nil {
					m.log("Step 2: WARNING: sync failed: %v", err)
				}
				fd.Close()
			}()

			buf := make([]byte, DefaultPokeSize)
			for e := range tasks {
				n, err := pokeExtentRangeWithBuf(fd, e.Offset, e.Length, DefaultThinBlockSize, buf)
				if err != nil {
					m.log("Step 2: ERROR: poke extent off=%d len=%d: %v", e.Offset, e.Length, err)
					continue
				}
				atomic.AddUint64(&blocksPoked, n)
				atomic.AddUint64(&extentsDone, 1)
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start).Seconds()
	finalBlocks := atomic.LoadUint64(&blocksPoked)
	bytesPoked := finalBlocks * DefaultPokeSize
	giPoked := float64(bytesPoked) / (1024.0 * 1024.0 * 1024.0)
	m.log("Step 2: completed poking PXD thin mappings. Extents=%d, blocks_poked=%d, bytes_poked=%d (%.2f GiB), elapsed=%.1fs",
		len(extents), finalBlocks, bytesPoked, giPoked, elapsed)

	// Final sync
	m.log("Step 2: syncing device %s to ensure metadata is committed", dstInfo.DevPath)
	fd, err := os.OpenFile(dstInfo.DevPath, os.O_RDWR, 0)
	if err != nil {
		m.log("Step 2: WARNING: failed to open device for final sync: %v", err)
	} else {
		if err := fd.Sync(); err != nil {
			m.log("Step 2: WARNING: final sync failed: %v", err)
		}
		fd.Close()
		m.log("Step 2: device sync completed")
	}

	return nil
}

// -----------------------------
// Step 3: FA->backend mapping
// -----------------------------

// Mapping represents a thin pool mapping entry.
type Mapping struct {
	OriginBegin uint64
	DataBegin   uint64
	Length      uint64
}

// Member represents a RAID array member device.
type Member struct {
	Name            string
	DataOffsetBytes uint64
}

type thinSuperblockXML struct {
	XMLName       xml.Name        `xml:"superblock"`
	DataBlockSize string          `xml:"data_block_size,attr"`
	Devices       []thinDeviceXML `xml:"device"`
}

type thinDeviceXML struct {
	SingleMappings []singleMappingXML `xml:"single_mapping"`
	RangeMappings  []rangeMappingXML  `xml:"range_mapping"`
}

type singleMappingXML struct {
	OriginBlock string `xml:"origin_block,attr"`
	DataBlock   string `xml:"data_block,attr"`
}

type rangeMappingXML struct {
	OriginBegin string `xml:"origin_begin,attr"`
	DataBegin   string `xml:"data_begin,attr"`
	Length      string `xml:"length,attr"`
}

func parseThinDumpFile(path string) (int, uint64, []Mapping, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, nil, err
	}
	var sb thinSuperblockXML
	if err := xml.Unmarshal(data, &sb); err != nil {
		return 0, 0, nil, err
	}
	if sb.XMLName.Local != "superblock" {
		return 0, 0, nil, fmt.Errorf("unexpected root tag %s", sb.XMLName.Local)
	}
	if sb.DataBlockSize == "" {
		return 0, 0, nil, fmt.Errorf("missing data_block_size attr")
	}
	dbs, err := strconv.Atoi(sb.DataBlockSize)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("data_block_size parse error: %v", err)
	}
	dataBlockSizeSectors := dbs
	blockBytes := uint64(dbs) * 512

	var mappings []Mapping
	for _, dev := range sb.Devices {
		for _, sm := range dev.SingleMappings {
			o, err1 := strconv.ParseUint(sm.OriginBlock, 10, 64)
			d, err2 := strconv.ParseUint(sm.DataBlock, 10, 64)
			if err1 != nil || err2 != nil {
				continue
			}
			mappings = append(mappings, Mapping{
				OriginBegin: o,
				DataBegin:   d,
				Length:      1,
			})
		}
		for _, rm := range dev.RangeMappings {
			o, err1 := strconv.ParseUint(rm.OriginBegin, 10, 64)
			d, err2 := strconv.ParseUint(rm.DataBegin, 10, 64)
			l, err3 := strconv.ParseUint(rm.Length, 10, 64)
			if err1 != nil || err2 != nil || err3 != nil {
				continue
			}
			mappings = append(mappings, Mapping{
				OriginBegin: o,
				DataBegin:   d,
				Length:      l,
			})
		}
	}
	sort.Slice(mappings, func(i, j int) bool {
		return mappings[i].OriginBegin < mappings[j].OriginBegin
	})
	return dataBlockSizeSectors, blockBytes, mappings, nil
}

func buildMappingIndex(mappings []Mapping) []uint64 {
	starts := make([]uint64, len(mappings))
	for i, m := range mappings {
		starts[i] = m.OriginBegin
	}
	return starts
}

func originToDataBlock(originBlock uint64, mappings []Mapping, originStarts []uint64) (uint64, bool) {
	idx := sort.Search(len(originStarts), func(i int) bool {
		return originStarts[i] > originBlock
	}) - 1
	if idx < 0 || idx >= len(mappings) {
		return 0, false
	}
	m := mappings[idx]
	if originBlock < m.OriginBegin || originBlock >= m.OriginBegin+m.Length {
		return 0, false
	}
	return m.DataBegin + (originBlock - m.OriginBegin), true
}

func mapOriginBlockToMember(originBlock uint64,
	dataBlockSizeSectors int,
	tdataStartSector uint64,
	mdChunkSizeBytes uint64,
	members []Member,
	mappings []Mapping,
	originStarts []uint64) (int, uint64, error) {

	dataBlock, ok := originToDataBlock(originBlock, mappings, originStarts)
	if !ok {
		return -1, 0, fmt.Errorf("no thin mapping for origin_block %d", originBlock)
	}
	sectorOffsetTdata := uint64(dataBlock) * uint64(dataBlockSizeSectors)
	mdSector := tdataStartSector + sectorOffsetTdata
	mdOffsetBytes := mdSector * 512

	N := uint64(len(members))
	chunk := mdChunkSizeBytes
	chunkNum := mdOffsetBytes / chunk
	chunkOff := mdOffsetBytes % chunk

	diskIndex := int(chunkNum % N)
	diskChunkIndex := chunkNum / N
	diskDataOffset := diskChunkIndex*chunk + chunkOff

	member := members[diskIndex]
	memberBase := member.DataOffsetBytes + diskDataOffset
	return diskIndex, memberBase, nil
}

func getPoolPrefixAndThinID(volID string) (string, int, error) {
	out, err := runCmd("dmsetup", "table")
	if err != nil {
		return "", 0, err
	}
	sc := bufio.NewScanner(strings.NewReader(out))
	for sc.Scan() {
		line := sc.Text()
		if !strings.Contains(line, volID) {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		nameField := strings.TrimSuffix(parts[0], ":")
		thStr := parts[len(parts)-1]
		thID, err := strconv.Atoi(thStr)
		if err != nil {
			continue
		}
		prefix := nameField
		if idx := strings.Index(prefix, "-"); idx > 0 {
			prefix = prefix[:idx]
		}
		return prefix, thID, nil
	}
	if err := sc.Err(); err != nil {
		return "", 0, err
	}
	return "", 0, fmt.Errorf("could not find dmsetup table entry containing Volume ID %s", volID)
}

func genThinDumpXML(poolPrefix string, thinID int, volName, volID string) (string, error) {
	tpoolDev := fmt.Sprintf("/dev/mapper/%s-pxpool-tpool", poolPrefix)
	tmetaDev := fmt.Sprintf("/dev/mapper/%s-pxpool_tmeta", poolPrefix)
	xmlName := fmt.Sprintf("thin_pxd%s_%s.xml", volName, volID)

	_, err := runCmd("dmsetup", "message", tpoolDev, "0", "reserve_metadata_snap")
	if err != nil {
		return "", fmt.Errorf("reserve_metadata_snap failed: %w", err)
	}
	defer runCmd("dmsetup", "message", tpoolDev, "0", "release_metadata_snap")

	f, err := os.Create(xmlName)
	if err != nil {
		return "", err
	}
	defer f.Close()

	cmdArgs := []string{"runc", "exec", "portworx",
		"thin_dump", "-m", "--dev-id", strconv.Itoa(thinID), tmetaDev}
	wrappedArgs := wrapWithNsenter(cmdArgs)

	cmd := exec.Command(wrappedArgs[0], wrappedArgs[1:]...)
	cmd.Stdout = f
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("thin_dump failed: %w", err)
	}
	return xmlName, nil
}

func getTdataStartSector(poolPrefix string) (uint64, error) {
	tdataDev := fmt.Sprintf("/dev/mapper/%s-pxpool_tdata", poolPrefix)
	out, err := runCmd("dmsetup", "table", tdataDev)
	if err != nil {
		return 0, err
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) == 0 {
		return 0, fmt.Errorf("empty dmsetup table for %s", tdataDev)
	}
	parts := strings.Fields(lines[0])
	if len(parts) < 5 {
		return 0, fmt.Errorf("unexpected dmsetup format: %s", lines[0])
	}
	sec, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		return 0, err
	}
	return sec, nil
}

func getMDDeviceAndVG(poolPrefix string) (string, string, error) {
	out, err := runCmd("runc", "exec", "portworx", "pvs")
	if err != nil {
		return "", "", err
	}
	sc := bufio.NewScanner(strings.NewReader(out))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "PV") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		pv, vg := parts[0], parts[1]
		if vg == poolPrefix {
			return pv, vg, nil
		}
	}
	if err := sc.Err(); err != nil {
		return "", "", err
	}
	return "", "", fmt.Errorf("could not find PV with VG=%s from pvs", poolPrefix)
}

func getMemberDataOffsetBytes(dev string) (uint64, error) {
	out, err := runCmd("mdadm", "-E", dev)
	if err != nil {
		return 0, fmt.Errorf("mdadm -E %s failed: %w", dev, err)
	}
	sc := bufio.NewScanner(strings.NewReader(out))
	for sc.Scan() {
		line := sc.Text()
		if strings.Contains(line, "Data Offset") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) != 2 {
				continue
			}
			fields := strings.Fields(strings.TrimSpace(parts[1]))
			if len(fields) == 0 {
				continue
			}
			sectors, err := strconv.ParseUint(fields[0], 10, 64)
			if err != nil {
				continue
			}
			return sectors * 512, nil
		}
	}
	if err := sc.Err(); err != nil {
		return 0, err
	}
	return 0, nil
}

func getMDLayout(mdDev string) (uint64, []Member, error) {
	out, err := runCmd("mdadm", "-D", mdDev)
	if err != nil {
		return 0, nil, err
	}
	var chunkStr string
	var memberDevs []string

	lines := strings.Split(out, "\n")
	inTable := false
	for _, line := range lines {
		l := strings.TrimSpace(line)
		if l == "" {
			if inTable {
				inTable = false
			}
			continue
		}
		if strings.Contains(l, "Chunk Size") {
			parts := strings.SplitN(l, ":", 2)
			if len(parts) == 2 {
				fields := strings.Fields(parts[1])
				if len(fields) > 0 {
					chunkStr = fields[0]
				}
			}
		}
		if strings.HasPrefix(l, "Number   Major") {
			inTable = true
			continue
		}
		if inTable {
			parts := strings.Fields(l)
			if len(parts) >= 6 {
				dev := parts[len(parts)-1]
				if strings.HasPrefix(dev, "/dev/") {
					memberDevs = append(memberDevs, dev)
				}
			}
		}
	}
	if chunkStr == "" {
		return 0, nil, fmt.Errorf("could not parse Chunk Size from mdadm -D output")
	}
	chunkBytes, err := parseSize(chunkStr)
	if err != nil {
		return 0, nil, fmt.Errorf("parse chunk size: %v", err)
	}
	if len(memberDevs) == 0 {
		return 0, nil, fmt.Errorf("no RAID member devices from mdadm -D")
	}
	var members []Member
	for _, d := range memberDevs {
		off, err := getMemberDataOffsetBytes(d)
		if err != nil {
			return 0, nil, err
		}
		members = append(members, Member{Name: d, DataOffsetBytes: off})
	}
	return chunkBytes, members, nil
}

func coalesceExtents(exts []Extent) []Extent {
	if len(exts) == 0 {
		return exts
	}
	sort.Slice(exts, func(i, j int) bool {
		return exts[i].Offset < exts[j].Offset
	})
	merged := make([]Extent, 0, len(exts))
	cur := exts[0]
	for _, e := range exts[1:] {
		if e.Offset == cur.Offset+cur.Length {
			cur.Length += e.Length
		} else {
			merged = append(merged, cur)
			cur = e
		}
	}
	merged = append(merged, cur)
	return merged
}

func (m *migrator) runStep3BuildMapping(srcPxVol, dstPxVol string) (string, string, error) {
	m.log("===== STEP 3/4: Build FA->backend mapping =====")
	m.log("Step 3: building FA->backend mapping for destination PX volume %q", dstPxVol)

	dstInfo, err := getPxVolInfoWithDevice(dstPxVol)
	if err != nil {
		return "", "", fmt.Errorf("step3: getPxVolInfo: %w", err)
	}

	poolPrefix, thinID, err := getPoolPrefixAndThinID(dstInfo.VolID)
	if err != nil {
		return "", "", fmt.Errorf("step3: getPoolPrefixAndThinID: %w", err)
	}
	m.log("Step 3: pool prefix=%s, thin dev ID=%d", poolPrefix, thinID)

	thinXML, err := genThinDumpXML(poolPrefix, thinID, dstInfo.Name, dstInfo.VolID)
	if err != nil {
		return "", "", fmt.Errorf("step3: genThinDumpXML: %w", err)
	}
	m.log("Step 3: thin_dump xml written to %s", thinXML)

	tdataStartSector, err := getTdataStartSector(poolPrefix)
	if err != nil {
		return "", "", fmt.Errorf("step3: getTdataStartSector: %w", err)
	}
	m.log("Step 3: tdata_start_sector=%d", tdataStartSector)

	mdDev, vg, err := getMDDeviceAndVG(poolPrefix)
	if err != nil {
		return "", "", fmt.Errorf("step3: getMDDeviceAndVG: %w", err)
	}
	m.log("Step 3: md device=%s (VG=%s)", mdDev, vg)

	mdChunkSizeBytes, members, err := getMDLayout(mdDev)
	if err != nil {
		return "", "", fmt.Errorf("step3: getMDLayout: %w", err)
	}
	m.log("Step 3: md chunk size=%d bytes, members=%d", mdChunkSizeBytes, len(members))
	for i, mem := range members {
		m.log("Step 3: member[%d]=%s, data_offset_bytes=%d", i, mem.Name, mem.DataOffsetBytes)
	}

	extFile := fmt.Sprintf("%s.extents", srcPxVol)
	extents, err := loadExtents(extFile)
	if err != nil {
		return "", "", fmt.Errorf("step3: loadExtents %s: %w", extFile, err)
	}
	if len(extents) == 0 {
		return "", "", fmt.Errorf("step3: no extents found in %s", extFile)
	}

	dataBlockSizeSectors, blockBytes, mappings, err := parseThinDumpFile(thinXML)
	if err != nil {
		return "", "", fmt.Errorf("step3: parseThinDumpFile: %w", err)
	}
	originStarts := buildMappingIndex(mappings)

	detailedOut := fmt.Sprintf("%s_%s_%s_to_backend_map.txt", srcPxVol, dstInfo.Name, dstInfo.VolID)
	aggOut := fmt.Sprintf("%s_%s_%s_aggregated_backend_map.txt", srcPxVol, dstInfo.Name, dstInfo.VolID)

	m.log("Step 3: extents file=%s, detailed_out=%s, aggregated_out=%s", extFile, detailedOut, aggOut)
	m.log("Step 3: thin_block_size=%d bytes, number_of_extents=%d", blockBytes, len(extents))

	backendExtents := make(map[string][]Extent)
	for _, mem := range members {
		backendExtents[mem.Name] = []Extent{}
	}

	start := time.Now()

	df, err := os.Create(detailedOut)
	if err != nil {
		return "", "", fmt.Errorf("step3: create detailed file: %w", err)
	}
	defer df.Close()

	fmt.Fprintf(df, "# Detailed FA->backend mapping\n")
	fmt.Fprintf(df, "# Dest PX vol: %s (ID: %s)\n", dstInfo.Name, dstInfo.VolID)
	fmt.Fprintf(df, "# Src PX vol : %s\n", srcPxVol)
	fmt.Fprintf(df, "# Extents    : %s\n\n", extFile)

	for idx, e := range extents {
		fmt.Fprintf(df, "[%d/%d] logical_extent off=%d len=%d (~%.2f MiB)\n",
			idx+1, len(extents), e.Offset, e.Length, float64(e.Length)/(1024.0*1024.0))

		if e.Length == 0 {
			fmt.Fprintf(df, "  (zero length, skipping)\n\n")
			continue
		}

		cur := e.Offset
		end := e.Offset + e.Length
		segCount := 0

		for cur < end {
			originBlock := cur / blockBytes
			blockStart := originBlock * blockBytes
			offsetInBlock := cur - blockStart
			maxLenThisBlock := blockBytes - offsetInBlock
			segLen := maxLenThisBlock
			if remaining := end - cur; remaining < segLen {
				segLen = remaining
			}

			diskIdx, memberBase, err := mapOriginBlockToMember(
				originBlock,
				dataBlockSizeSectors,
				tdataStartSector,
				mdChunkSizeBytes,
				members,
				mappings,
				originStarts,
			)
			if err != nil {
				return "", "", fmt.Errorf("step3: %w", err)
			}
			member := members[diskIdx]
			dstOff := memberBase + offsetInBlock

			fmt.Fprintf(df, "  segment %d: member=%s, logical_off=%d, seg_len=%d, backend_off=%d\n",
				segCount, member.Name, cur, segLen, dstOff)
			backendExtents[member.Name] = append(backendExtents[member.Name], Extent{
				Offset: dstOff,
				Length: segLen,
			})
			segCount++
			cur += segLen
		}
		fmt.Fprintln(df)
	}

	df.Sync()

	// aggregated
	af, err := os.Create(aggOut)
	if err != nil {
		return "", "", fmt.Errorf("step3: create aggregated file: %w", err)
	}
	defer af.Close()

	fmt.Fprintf(af, "# Aggregated backend extents (coalesced per device)\n")
	fmt.Fprintf(af, "# Dest PX vol: %s (ID: %s)\n", dstInfo.Name, dstInfo.VolID)
	fmt.Fprintf(af, "# Src PX vol : %s\n\n", srcPxVol)

	for _, mem := range members {
		name := mem.Name
		segs := backendExtents[name]
		merged := coalesceExtents(segs)
		fmt.Fprintf(af, "%s:\n", name)
		if len(merged) == 0 {
			fmt.Fprintf(af, "  (no extents)\n\n")
			continue
		}
		for _, ex := range merged {
			fmt.Fprintf(af, "  off=%d len=%d\n", ex.Offset, ex.Length)
		}
		fmt.Fprintln(af)
	}

	elapsed := time.Since(start).Seconds()
	m.log("Step 3: mapping completed in %.1fs", elapsed)
	m.log("Step 3: mapping complete. Detailed=%s, Aggregated=%s", detailedOut, aggOut)
	return detailedOut, aggOut, nil
}

// -----------------------------
// Step 4: Copy using XCOPY
// -----------------------------

// MapSegment represents a segment from the mapping file.
type MapSegment struct {
	Member string
	SrcOff uint64
	DstOff uint64
	Length uint64
	HdrIdx int
}

// CopyTask represents an XCOPY task.
type CopyTask struct {
	Member string
	SrcOff uint64
	DstOff uint64
	Length uint64
}

// XcopyStats tracks XCOPY statistics.
type XcopyStats struct {
	SegmentsCompleted     uint64
	TotalBytes            uint64
	TotalXcopyInvocations uint64
	TotalXcopyBlocks      uint64
}

var segRe = regexp.MustCompile(`segment\s+\d+:\s+member=(\S+),\s+logical_off=(\d+),\s+seg_len=(\d+),\s+backend_off=(\d+)`)

func parseMappingFile(path string, log LogFunc) ([]string, []MapSegment, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open mapping file: %w", err)
	}
	defer f.Close()

	var headers []string
	var segments []MapSegment

	sc := bufio.NewScanner(f)
	currentHdrIdx := -1
	for sc.Scan() {
		line := sc.Text()
		stripped := strings.TrimSpace(line)
		if stripped == "" || strings.HasPrefix(stripped, "#") {
			continue
		}
		if strings.HasPrefix(stripped, "[") && strings.Contains(stripped, "logical_extent") {
			headers = append(headers, stripped)
			currentHdrIdx = len(headers) - 1
			continue
		}
		m := segRe.FindStringSubmatch(stripped)
		if m != nil {
			member := m[1]
			srcOff, _ := strconv.ParseUint(m[2], 10, 64)
			segLen, _ := strconv.ParseUint(m[3], 10, 64)
			dstOff, _ := strconv.ParseUint(m[4], 10, 64)
			segments = append(segments, MapSegment{
				Member: member,
				SrcOff: srcOff,
				DstOff: dstOff,
				Length: segLen,
				HdrIdx: currentHdrIdx,
			})
		}
	}
	if err := sc.Err(); err != nil {
		return nil, nil, err
	}
	if len(segments) == 0 {
		log("Step 4: WARNING: no segments parsed from mapping file %s", path)
	}
	return headers, segments, nil
}

func getBlockSize(dev string) (int, error) {
	out, err := runCmd("blockdev", "--getss", dev)
	if err != nil {
		return 0, fmt.Errorf("blockdev --getss %s failed: %w", dev, err)
	}
	val := strings.TrimSpace(out)
	bs, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("parse block size: %v", err)
	}
	return bs, nil
}

func coalesceSegments(tasks []CopyTask) []CopyTask {
	if len(tasks) == 0 {
		return tasks
	}
	sorted := make([]CopyTask, len(tasks))
	copy(sorted, tasks)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Member != sorted[j].Member {
			return sorted[i].Member < sorted[j].Member
		}
		if sorted[i].SrcOff != sorted[j].SrcOff {
			return sorted[i].SrcOff < sorted[j].SrcOff
		}
		return sorted[i].DstOff < sorted[j].DstOff
	})

	var merged []CopyTask
	cur := sorted[0]
	for i := 1; i < len(sorted); i++ {
		s := sorted[i]
		if s.Member == cur.Member &&
			s.SrcOff == cur.SrcOff+cur.Length &&
			s.DstOff == cur.DstOff+cur.Length {
			cur.Length += s.Length
		} else {
			merged = append(merged, cur)
			cur = s
		}
	}
	merged = append(merged, cur)
	return merged
}

func xcopySegment(srcDev, dstDev string, srcOff, dstOff, length uint64, bs int, log LogFunc) error {
	if length == 0 {
		return nil
	}
	if srcOff%uint64(bs) != 0 || dstOff%uint64(bs) != 0 || length%uint64(bs) != 0 {
		log("Step 4: WARNING: XCOPY segment not %d-byte aligned (src_off=%d, dst_off=%d, len=%d) – skipping",
			bs, srcOff, dstOff, length)
		return nil
	}
	srcLBA := srcOff / uint64(bs)
	dstLBA := dstOff / uint64(bs)
	blocks := length / uint64(bs)
	if blocks == 0 {
		return nil
	}

	args := []string{
		"sg_xcopy",
		fmt.Sprintf("if=%s", srcDev),
		fmt.Sprintf("of=%s", dstDev),
		fmt.Sprintf("bs=%d", bs),
		fmt.Sprintf("skip=%d", srcLBA),
		fmt.Sprintf("seek=%d", dstLBA),
		fmt.Sprintf("count=%d", blocks),
		"id_usage=disable",
	}

	// Wrap command appropriately based on environment
	var wrappedArgs []string
	if needsNsenterForDevices() {
		// Need nsenter (container without host /dev)
		wrappedArgs = wrapWithNsenter(args)
	} else if isInContainer() {
		// In container with host /dev mounted, use chroot /host for libraries
		wrappedArgs = wrapWithChrootHost(args)
	} else {
		// On host, run directly
		wrappedArgs = args
	}

	cmd := exec.Command(wrappedArgs[0], wrappedArgs[1:]...)
	dn := getDevNull()
	cmd.Stdin = dn
	cmd.Stdout = dn
	cmd.Stderr = dn

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("sg_xcopy failed for src_off=%d, dst_off=%d, len=%d: %w",
			srcOff, dstOff, length, err)
	}
	return nil
}

func (m *migrator) runXcopyMultiWorker(
	segments []CopyTask,
	srcDev string,
	xcopyBS int,
	workers int,
) int {
	if workers < 1 {
		workers = 1
	}
	if workers > MaxXcopyWorkersCap {
		workers = MaxXcopyWorkersCap
	}

	tasks := make(chan CopyTask, len(segments))
	for _, t := range segments {
		tasks <- t
	}
	close(tasks)

	var stats XcopyStats
	var wg sync.WaitGroup
	wg.Add(workers)

	start := time.Now()
	totalSegs := len(segments)

	workerFn := func() {
		defer wg.Done()
		for task := range tasks {
			blocks := task.Length / uint64(xcopyBS)
			atomic.AddUint64(&stats.TotalXcopyInvocations, 1)
			atomic.AddUint64(&stats.TotalXcopyBlocks, blocks)

			if err := xcopySegment(
				srcDev,
				task.Member,
				task.SrcOff,
				task.DstOff,
				task.Length,
				xcopyBS,
				m.log,
			); err != nil {
				m.log("Step 4: ERROR: XCOPY failed for member=%s, src_off=%d, dst_off=%d, len=%d: %v",
					task.Member, task.SrcOff, task.DstOff, task.Length, err)
				continue
			}
			atomic.AddUint64(&stats.TotalBytes, task.Length)
			atomic.AddUint64(&stats.SegmentsCompleted, 1)
		}
	}

	for i := 0; i < workers; i++ {
		go workerFn()
	}

	if m.cfg.StatsInterval > 0 {
		ticker := time.NewTicker(time.Duration(m.cfg.StatsInterval) * time.Second)
		go func() {
			defer ticker.Stop()
			var prevSegDone uint64
			for range ticker.C {
				segDone := atomic.LoadUint64(&stats.SegmentsCompleted)
				if int(segDone) >= totalSegs {
					return
				}
				now := time.Now()
				elapsed := now.Sub(start).Seconds()
				bytes := atomic.LoadUint64(&stats.TotalBytes)

				deltaSeg := segDone - prevSegDone
				prevSegDone = segDone

				giB := float64(bytes) / (1024.0 * 1024.0 * 1024.0)
				miB := float64(bytes) / (1024.0 * 1024.0)
				throughputMiB := 0.0
				if elapsed > 0 {
					throughputMiB = miB / elapsed
				}

				m.log("Step 4: [STATS] Elapsed: %.1fs, segments: %d/%d (+%d), bytes: %d (%.2f GiB), avg throughput: %.2f MiB/s",
					elapsed, segDone, totalSegs, deltaSeg, bytes, giB, throughputMiB)

				blocks := atomic.LoadUint64(&stats.TotalXcopyBlocks)
				m.log("Step 4: [STATS] XCOPY so far: sg_xcopy_calls=%d, blocks=%d, bytes=%d",
					atomic.LoadUint64(&stats.TotalXcopyInvocations),
					blocks,
					blocks*uint64(xcopyBS))
			}
		}()
	}

	wg.Wait()

	elapsedTotal := time.Since(start).Seconds()
	bytes := atomic.LoadUint64(&stats.TotalBytes)

	giBTotal := float64(bytes) / (1024.0 * 1024.0 * 1024.0)
	miBTotal := float64(bytes) / (1024.0 * 1024.0)

	throughputMiB := 0.0
	if elapsedTotal > 0 {
		throughputMiB = miBTotal / elapsedTotal
	}

	m.log("Step 4: Total bytes via XCOPY: %d (%.2f GiB)", bytes, giBTotal)
	m.log("Step 4: Elapsed time: %.1fs, average throughput: %.2f MiB/s", elapsedTotal, throughputMiB)

	blocks := atomic.LoadUint64(&stats.TotalXcopyBlocks)
	m.log("Step 4: XCOPY stats: sg_xcopy_calls=%d, blocks=%d, bytes=%d",
		atomic.LoadUint64(&stats.TotalXcopyInvocations),
		blocks,
		blocks*uint64(xcopyBS))
	m.log("Step 4: XCOPY operations completed (sg_xcopy, multi-goroutine).")
	return 0
}

func (m *migrator) runStep4CopyData(srcPxVol, dstPxVol string) error {
	m.log("===== STEP 4/4: Copy data via XCOPY =====")

	srcInfo, err := getPxVolInfoWithDevice(srcPxVol)
	if err != nil {
		return fmt.Errorf("step4: getPxVolInfo(src): %w", err)
	}
	dstInfo, err := getPxVolInfoWithDevice(dstPxVol)
	if err != nil {
		return fmt.Errorf("step4: getPxVolInfo(dst): %w", err)
	}

	mappingFile := fmt.Sprintf("%s_%s_%s_to_backend_map.txt",
		srcPxVol, dstInfo.Name, dstInfo.VolID)
	copyLogFile := fmt.Sprintf("%s_%s_%s_copy_segments.txt",
		srcPxVol, dstInfo.Name, dstInfo.VolID)

	m.log("Step 4: Source PX vol: %s (arg: %s, ID: %s)", srcInfo.Name, srcPxVol, srcInfo.VolID)
	m.log("Step 4: Source device path: %s", srcInfo.DevPath)
	m.log("Step 4: Destination PX vol: %s (arg: %s, ID: %s)", dstInfo.Name, dstPxVol, dstInfo.VolID)
	m.log("Step 4: Mapping file: %s", mappingFile)
	m.log("Step 4: Copy log file: %s", copyLogFile)

	if _, err := os.Stat(mappingFile); err != nil {
		m.log("Failed to stat mapping file: %v", err)
		return fmt.Errorf("step4: mapping file '%s' not found", mappingFile)
	}

	// Check if sg_xcopy is available - wrap with nsenter if in container
	checkArgs := wrapWithNsenter([]string{"bash", "-lc", "command -v sg_xcopy"})
	if err := exec.Command(checkArgs[0], checkArgs[1:]...).Run(); err != nil {
		return fmt.Errorf("step4: sg_xcopy not found in PATH")
	}

	headers, segments, err := parseMappingFile(mappingFile, m.log)
	if err != nil {
		m.log("Failed to parse mapping file: %v", err)
		return fmt.Errorf("step4: parseMappingFile: %w", err)
	}
	if len(segments) == 0 {
		m.log("Step 4: no segments to copy; exiting.")
		return nil
	}

	segmentsPerHdr := make(map[int]int)
	memberDevs := make(map[string]struct{})
	for _, s := range segments {
		if s.HdrIdx >= 0 {
			segmentsPerHdr[s.HdrIdx]++
		}
		memberDevs[s.Member] = struct{}{}
	}

	m.log("Step 4: Backend member devices referenced in mapping:")
	for d := range memberDevs {
		m.log("Step 4:   %s", d)
	}

	srcBS, err := getBlockSize(srcInfo.DevPath)
	if err != nil {
		return fmt.Errorf("step4: %v", err)
	}
	for d := range memberDevs {
		bs, err := getBlockSize(d)
		if err != nil {
			return fmt.Errorf("step4: %v", err)
		}
		if bs != srcBS {
			return fmt.Errorf("step4: logical block size mismatch src=%d, %s=%d", srcBS, d, bs)
		}
	}
	m.log("Step 4: XCOPY using logical block size %d bytes (from devices)", srcBS)

	// summary only (no per-extent spam)
	_ = segmentsPerHdr // kept in case we want richer summary later
	m.log("Step 4: Logical extents: %d, backend segments: %d", len(headers), len(segments))

	// build CopyTask list & coalesce
	baseSegs := make([]CopyTask, 0, len(segments))
	var totalBytes uint64
	for _, s := range segments {
		baseSegs = append(baseSegs, CopyTask{
			Member: s.Member,
			SrcOff: s.SrcOff,
			DstOff: s.DstOff,
			Length: s.Length,
		})
		totalBytes += s.Length
	}
	coalesced := coalesceSegments(baseSegs)
	giB := float64(totalBytes) / (1024.0 * 1024.0 * 1024.0)
	m.log("Step 4: segments from mapping: %d, after coalesce: %d, total_bytes=%d (~%.2f GiB)",
		len(baseSegs), len(coalesced), totalBytes, giB)

	logF, err := os.Create(copyLogFile)
	if err != nil {
		return fmt.Errorf("step4: opening copy log file: %v", err)
	}
	defer logF.Close()

	for _, t := range coalesced {
		blocks := t.Length / uint64(srcBS)
		fmt.Fprintf(logF, "XCOPY src_off=%d len=%d -> %s dst_off=%d (blocks=%d)\n",
			t.SrcOff, t.Length, t.Member, t.DstOff, blocks)
	}
	logF.Sync()
	m.log("Step 4: detailed copy log will be written to: %s", copyLogFile)

	workers := m.cfg.Jobs
	if workers <= 0 {
		workers = DefaultStep4Workers
	}
	if workers > len(coalesced) {
		workers = len(coalesced)
	}
	if workers < 1 {
		workers = 1
	}

	m.log("Step 4: XCOPY worker goroutines: %d", workers)

	ret := m.runXcopyMultiWorker(coalesced, srcInfo.DevPath, srcBS, workers)
	m.log("Step 4: Detailed copy log written to: %s", copyLogFile)

	if devNull != nil {
		devNull.Close()
	}
	if ret != 0 {
		return fmt.Errorf("step4: XCOPY run returned %d", ret)
	}
	return nil
}
