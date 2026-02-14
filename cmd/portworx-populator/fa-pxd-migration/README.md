# FA to PXD Migration Tool

Consolidated Go binary for migrating FlashArray (FA) volumes to Portworx (PXD) volumes using extent-based XCOPY.

## Overview

This tool replaces the multi-step Python script approach with a single, efficient Go binary that performs all migration steps:

1. **Fetch FA diff extents** - Query FlashArray API for allocated blocks
2. **Poke PXD thin mapping** - Pre-allocate thin blocks on destination volume
3. **Build FA→backend mapping** - Map FA extents to backend device offsets
4. **Copy data using XCOPY** - Efficient storage-to-storage data transfer

## Usage

```bash
./fa_pxd_migration [OPTIONS] <src_px_vol> <dst_px_vol>
```

### Options

- `-dry-run` - Global dry-run (no device writes / XCOPY)
- `-step N` - Run only step N (1..4). Requires pre-req files from earlier steps
- `-max-step N` - Run steps 1..N (pipeline style)
- `-jobs N` - XCOPY goroutines for step 4 (default: 512)
- `-step2-workers N` - PXD poke workers for step 2 (default: 16, capped by #extents)
- `-stats-interval S` - Stats interval in seconds for step 2 & step 4 (0 = disable)

### FlashArray Configuration

Set via environment variables or command-line flags:

- `-fa-ip string` - FlashArray management IP/hostname (or `FA_IP` env)
- `-fa-api-ver string` - FlashArray API version (e.g. 2.41) or `FA_API_VER` env
- `-fa-api-token string` - FlashArray API token or `FA_API_TOKEN` env

### Examples

```bash
# Execute all migration steps
export FA_IP="js500-9.dev.purestorage.com"
export FA_API_VER="2.41"
export FA_API_TOKEN="53321f38-6416-7356-cba1-6fe60bb02c9b"
./fa_pxd_migration pvc-source-fa pvc-dest-pxd

# Execute only step 4 (data copy) - requires previous steps completed
./fa_pxd_migration -step 4 pvc-source-fa pvc-dest-pxd

# Dry run with custom workers
./fa_pxd_migration -dry-run -jobs 1024 -step2-workers 32 pvc-source-fa pvc-dest-pxd
```

## Prerequisites

- Source FA volume and destination PXD volume must be created and attached
- `pxctl` must be available in PATH
- `sg_xcopy` must be available for XCOPY operations
- Sufficient permissions to access block devices

## Output Files

The tool generates several files during execution:

- `<src>.extents` - FA diff extents (offset, length pairs)
- `<src>_<dst>_<vol_id>_to_backend_map.txt` - Detailed backend mapping
- `<src>_<dst>_<vol_id>_aggregated_backend_map.txt` - Aggregated mapping
- `<src>_<dst>_<vol_id>_copy_segments.txt` - XCOPY operation log

## Performance Tuning

- **`-jobs`**: Increase for faster XCOPY (default: 512, max: 4096)
- **`-step2-workers`**: Increase for faster poke (default: 16)
- **`-stats-interval`**: Set to 0 to disable stats (reduces overhead)

## Building

```bash
# From repository root
go build -o fa_pxd_migration ./pkg/migration
```

## Integration

This tool is integrated into the conversion pod via the Dockerfile and called by the Ansible playbook.

