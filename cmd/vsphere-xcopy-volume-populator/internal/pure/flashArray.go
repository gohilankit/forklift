package pure

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/kubev2v/forklift/cmd/vsphere-xcopy-volume-populator/internal/fcutil"
	"github.com/kubev2v/forklift/cmd/vsphere-xcopy-volume-populator/internal/populator"
	"github.com/kubev2v/forklift/cmd/vsphere-xcopy-volume-populator/internal/vmware"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
	corev1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

const FlashProviderID = "624a9370"

// Ensure FlashArrayClonner implements required interfaces
var _ populator.RDMCapable = &FlashArrayClonner{}
var _ populator.VVolCapable = &FlashArrayClonner{}
var _ populator.VMDKCapable = &FlashArrayClonner{}

type FlashArrayClonner struct {
	restClient    *RestClient
	clusterPrefix string
}

const ClusterPrefixEnv = "PURE_CLUSTER_PREFIX"
const helpMessage = `clusterPrefix is missing and PURE_CLUSTER_PREFIX is not set.
Use this to extract the value:
printf "px_%s" $(oc get storagecluster -A -o=jsonpath='{.items[0].status.clusterUid}'| head -c 8)
`

// NewFlashArrayClonner creates a new FlashArrayClonner
// Authentication is mutually exclusive:
// - If apiToken is provided (non-empty), it will be used for authentication (username/password ignored)
// - If apiToken is empty, username and password will be used for authentication
func NewFlashArrayClonner(hostname, username, password, apiToken string, skipSSLVerification bool, clusterPrefix string) (FlashArrayClonner, error) {
	if clusterPrefix == "" {
		return FlashArrayClonner{}, errors.New(helpMessage)
	}

	// Create the REST client for all operations
	restClient, err := NewRestClient(hostname, username, password, apiToken, skipSSLVerification)
	if err != nil {
		return FlashArrayClonner{}, fmt.Errorf("failed to create REST client: %w", err)
	}

	return FlashArrayClonner{
		restClient:    restClient,
		clusterPrefix: clusterPrefix,
	}, nil
}

// EnsureClonnerIgroup creates or updates an initiator group with the ESX adapters
// Named hgroup in flash terminology
func (f *FlashArrayClonner) EnsureClonnerIgroup(initiatorGroup string, esxAdapters []string) (populator.MappingContext, error) {
	// pure does not allow a single host to connect to 2 separae groups. Hence
	// we must connect map the volume to the host, and not to the group
	hosts, err := f.restClient.ListHosts()
	if err != nil {
		return nil, err
	}
	for _, h := range hosts {
		klog.Infof("checking host %s, iqns: %v, wwns: %v", h.Name, h.Iqn, h.Wwn)
		for _, wwn := range h.Wwn {
			for _, hostAdapter := range esxAdapters {
				if !strings.HasPrefix(hostAdapter, "fc.") {
					continue
				}
				adapterWWPN, err := fcUIDToWWPN(hostAdapter)
				if err != nil {
					klog.Warningf("failed to extract WWPN from adapter %s: %s", hostAdapter, err)
					continue
				}

				// Compare WWNs using the utility function that normalizes formatting
				klog.Infof("comparing ESX adapter WWPN %s with Pure host WWN %s", adapterWWPN, wwn)
				if fcutil.CompareWWNs(adapterWWPN, wwn) {
					klog.Infof("match found. Adding host %s to mapping context.", h.Name)
					return populator.MappingContext{"hosts": []string{h.Name}}, nil
				}
			}
		}
		for _, iqn := range h.Iqn {
			if slices.Contains(esxAdapters, iqn) {
				klog.Infof("adding host to group %v", h.Name)
				return populator.MappingContext{"hosts": []string{h.Name}}, nil
			}
		}

	}
	return nil, fmt.Errorf("no hosts found matching any of the provided IQNs/FC adapters: %v", esxAdapters)
}

// Map is responsible to mapping an initiator group to a populator.LUN
func (f *FlashArrayClonner) Map(
	initatorGroup string,
	targetLUN populator.LUN,
	context populator.MappingContext) (populator.LUN, error) {
	hosts, ok := context["hosts"]
	if !ok {
		return populator.LUN{}, fmt.Errorf("hosts not found in context")
	}
	hs, ok := hosts.([]string)
	if !ok || len(hs) == 0 {
		return populator.LUN{}, errors.New("invalid or empty hosts list in mapping context")
	}
	for _, host := range hs {
		klog.Infof("connecting host %s to volume %s", host, targetLUN.Name)
		err := f.restClient.ConnectHost(host, targetLUN.Name)
		if err != nil {
			if strings.Contains(err.Error(), "Connection already exists.") {
				continue
			}
			return populator.LUN{}, fmt.Errorf("connect host %q to volume %q: %w", host, targetLUN.Name, err)
		}

		return targetLUN, nil
	}
	return populator.LUN{}, fmt.Errorf("connection failed for all hosts in context")
}

// UnMap is responsible to unmapping an initiator group from a populator.LUN
func (f *FlashArrayClonner) UnMap(initatorGroup string, targetLUN populator.LUN, context populator.MappingContext) error {
	hosts, ok := context["hosts"]

	if ok {
		hs, ok := hosts.([]string)
		if ok && len(hs) > 0 {
			for _, host := range hs {
				klog.Infof("disconnecting host %s from volume %s", host, targetLUN.Name)
				err := f.restClient.DisconnectHost(host, targetLUN.Name)
				if err != nil {
					return err
				}

			}
		}
	}
	return nil
}

// CurrentMappedGroups returns the initiator groups the populator.LUN is mapped to
func (f *FlashArrayClonner) CurrentMappedGroups(targetLUN populator.LUN, context populator.MappingContext) ([]string, error) {
	// we don't use the host group feature, as a host in pure flasharray can not belong to two separate groups, and we
	// definitely don't want to break host from their current groups. insted we'll just map/unmap the volume to individual hosts
	return nil, nil
}

// ResolvePVToLUN resolves a PersistentVolume to Pure FlashArray LUN details
func (f *FlashArrayClonner) ResolvePVToLUN(pv populator.PersistentVolume) (populator.LUN, error) {
	pvVolumeHandle := pv.VolumeHandle
	v, err := f.restClient.GetVolumeById(pvVolumeHandle)
	if err != nil {
		if strings.Contains(err.Error(), "Volume does not exist.") {
			klog.Errorf("Volume with handle %s does not exist: %v. Trying with Volume Name", pvVolumeHandle, err)
			volumeName := fmt.Sprintf("%s-%s", f.clusterPrefix, pv.Name)
			v, err = f.restClient.GetVolume(volumeName)
			if err != nil {
				return populator.LUN{}, fmt.Errorf("failed to get volume by name %s: %w", volumeName, err)
			}
		}
	}

	klog.Infof("volume %+v\n", v)
	l := populator.LUN{Name: v.Name, SerialNumber: v.Serial, NAA: fmt.Sprintf("naa.%s%s", FlashProviderID, strings.ToLower(v.Serial))}

	return l, nil
}

// fcUIDToWWPN extracts the WWPN (port name) from an ESXi fcUid string.
// The expected input is of the form: 'fc.WWNN:WWPN' where the WWNN and WWPN
// are not separated with columns every byte (2 hex chars) like 00:00:00:00:00:00:00:00
func fcUIDToWWPN(fcUid string) (string, error) {
	return fcutil.ExtractAndFormatWWPN(fcUid)
}

// VvolCopy performs a direct copy operation using vSphere API to discover source volume
func (f *FlashArrayClonner) VvolCopy(vsphereClient vmware.Client, vmId string, sourceVMDKFile string, persistentVolume populator.PersistentVolume, progress chan<- uint64) error {
	klog.Infof("Starting VVol copy operation for VM %s", vmId)

	// Parse the VMDK path
	vmDisk, err := populator.ParseVmdkPath(sourceVMDKFile)
	if err != nil {
		return fmt.Errorf("failed to parse VMDK path: %w", err)
	}

	// Resolve target volume details
	targetLUN, err := f.ResolvePVToLUN(persistentVolume)
	if err != nil {
		return fmt.Errorf("failed to resolve target volume: %w", err)
	}

	// Try to get source volume from vSphere API
	sourceVolume, err := f.getSourceVolume(vsphereClient, vmId, vmDisk)
	if err != nil {
		return fmt.Errorf("failed to get source volume from vSphere: %w", err)
	}

	klog.Infof("Copying from source volume %s to target volume %s", sourceVolume, targetLUN.Name)

	// Perform the copy operation
	err = f.performVolumeCopy(sourceVolume, targetLUN.Name, progress)
	if err != nil {
		return fmt.Errorf("copy operation failed: %w", err)
	}

	klog.Infof("VVol copy operation completed successfully")
	return nil
}

// getSourceVolume find the Pure volume name for a VMDK
func (f *FlashArrayClonner) getSourceVolume(vsphereClient vmware.Client, vmId string, vmDisk populator.VMDisk) (string, error) {
	ctx := context.Background()

	// Get VM object from vSphere
	finder := find.NewFinder(vsphereClient.(*vmware.VSphereClient).Client.Client, true)
	vm, err := finder.VirtualMachine(ctx, vmId)
	if err != nil {
		return "", fmt.Errorf("failed to get VM: %w", err)
	}

	// Get VM hardware configuration
	var vmObject mo.VirtualMachine
	pc := property.DefaultCollector(vsphereClient.(*vmware.VSphereClient).Client.Client)
	err = pc.RetrieveOne(ctx, vm.Reference(), []string{"config.hardware.device"}, &vmObject)
	if err != nil {
		return "", fmt.Errorf("failed to get VM hardware config: %w", err)
	}

	// Look through VM's virtual disks to find VVol backing
	if vmObject.Config == nil || vmObject.Config.Hardware.Device == nil {
		return "", fmt.Errorf("VM config or hardware devices not found")
	}

	for _, device := range vmObject.Config.Hardware.Device {
		if disk, ok := device.(*types.VirtualDisk); ok {
			if backing, ok := disk.Backing.(*types.VirtualDiskFlatVer2BackingInfo); ok {
				// Check if this is a VVol backing and matches our target VMDK
				if backing.BackingObjectId != "" && f.matchesVMDKPath(backing.FileName, vmDisk) {
					klog.Infof("Found VVol backing for VMDK %s with ID %s", vmDisk.VmdkFile, backing.BackingObjectId)

					// Use REST client to find the volume by VVol ID
					volumeName, err := f.restClient.FindVolumeByVVolID(backing.BackingObjectId)
					if err != nil {
						klog.Warningf("Failed to find volume by VVol ID %s: %v", backing.BackingObjectId, err)
						continue
					}

					return volumeName, nil
				}
			}
		}
	}

	return "", fmt.Errorf("VVol backing for VMDK %s not found", vmDisk.VmdkFile)
}

// matchesVMDKPath checks if a vSphere VVol filename matches the target VMDK
func (f *FlashArrayClonner) matchesVMDKPath(fileName string, vmDisk populator.VMDisk) bool {
	fileBase := filepath.Base(fileName)
	targetBase := filepath.Base(vmDisk.VmdkFile)
	return fileBase == targetBase
}

// RDMCopy performs a copy operation for RDM-backed disks using Pure FlashArray APIs
func (f *FlashArrayClonner) RDMCopy(vsphereClient vmware.Client, vmId string, sourceVMDKFile string, persistentVolume populator.PersistentVolume, progress chan<- uint64) error {
	klog.Infof("Pure RDM Copy: Starting RDM copy operation for VM %s", vmId)

	// Get disk backing info to find the RDM device
	backing, err := vsphereClient.GetVMDiskBacking(context.Background(), vmId, sourceVMDKFile)
	if err != nil {
		return fmt.Errorf("failed to get RDM disk backing info: %w", err)
	}

	if !backing.IsRDM {
		return fmt.Errorf("disk %s is not an RDM disk", sourceVMDKFile)
	}

	klog.Infof("Pure RDM Copy: Found RDM device: %s", backing.DeviceName)

	// Resolve the source LUN from the RDM device name
	sourceLUN, err := f.resolveRDMToLUN(backing.DeviceName)
	if err != nil {
		return fmt.Errorf("failed to resolve RDM device to source LUN: %w", err)
	}

	// Resolve the target PV to LUN
	targetLUN, err := f.ResolvePVToLUN(persistentVolume)
	if err != nil {
		return fmt.Errorf("failed to resolve target volume: %w", err)
	}

	klog.Infof("Pure RDM Copy: Copying from source LUN %s to target LUN %s", sourceLUN.Name, targetLUN.Name)

	// Report progress start
	progress <- 10

	// Perform the copy operation using Pure FlashArray API
	err = f.restClient.CopyVolume(sourceLUN.Name, targetLUN.Name)
	if err != nil {
		return fmt.Errorf("Pure FlashArray CopyVolume failed: %w", err)
	}

	// Report progress complete
	progress <- 100

	klog.Infof("Pure RDM Copy: Copy operation completed successfully")
	return nil
}

// resolveRDMToLUN resolves an RDM device name to a Pure FlashArray LUN
func (f *FlashArrayClonner) resolveRDMToLUN(deviceName string) (populator.LUN, error) {
	klog.Infof("Pure RDM Copy: Resolving RDM device %s to LUN", deviceName)

	// The device name from RDM typically contains the NAA identifier
	// For Pure FlashArray, format is "naa.624a9370<serial>" where 624a9370 is the FlashProviderID
	// We need to extract the serial number and find the corresponding LUN

	// Extract serial number from NAA identifier
	serial, err := extractSerialFromNAA(deviceName)
	if err != nil {
		// Try to find by listing all volumes and matching
		klog.Warningf("Could not extract serial from NAA %s: %v, trying to find by listing volumes", deviceName, err)
		return f.findVolumeByDeviceName(deviceName)
	}

	// Find volume by serial number
	volume, err := f.restClient.FindVolumeBySerial(serial)
	if err != nil {
		return populator.LUN{}, fmt.Errorf("failed to find volume by serial %s: %w", serial, err)
	}

	klog.Infof("Pure RDM Copy: Found matching volume %s for device %s", volume.Name, deviceName)
	return populator.LUN{
		Name:         volume.Name,
		SerialNumber: volume.Serial,
		NAA:          fmt.Sprintf("naa.%s%s", FlashProviderID, strings.ToLower(volume.Serial)),
	}, nil
}

// extractSerialFromNAA extracts the serial number from a NAA identifier
// NAA format for Pure: naa.624a9370<serial> where serial is the volume serial
func extractSerialFromNAA(naa string) (string, error) {
	naa = strings.ToLower(naa)

	// Remove "naa." prefix if present
	naa = strings.TrimPrefix(naa, "naa.")

	// Check if it starts with Pure's provider ID
	providerIDLower := strings.ToLower(FlashProviderID)
	if !strings.HasPrefix(naa, providerIDLower) {
		return "", fmt.Errorf("NAA %s does not appear to be a Pure FlashArray device (expected prefix %s)", naa, FlashProviderID)
	}

	// Extract serial (everything after the provider ID)
	serial := strings.TrimPrefix(naa, providerIDLower)
	if serial == "" {
		return "", fmt.Errorf("could not extract serial from NAA %s", naa)
	}

	return strings.ToUpper(serial), nil
}

// findVolumeByDeviceName finds a volume by searching through all volumes
func (f *FlashArrayClonner) findVolumeByDeviceName(deviceName string) (populator.LUN, error) {
	// List all volumes and find the one matching the device name
	volumes, err := f.restClient.ListVolumes()
	if err != nil {
		return populator.LUN{}, fmt.Errorf("failed to list volumes: %w", err)
	}

	deviceName = strings.ToLower(deviceName)

	for _, volume := range volumes {
		// Build the expected NAA for this volume
		naa := fmt.Sprintf("naa.%s%s", FlashProviderID, strings.ToLower(volume.Serial))

		// Compare with the device name
		if strings.Contains(deviceName, strings.ToLower(volume.Serial)) ||
			strings.Contains(deviceName, naa) ||
			deviceName == naa {
			klog.Infof("Pure RDM Copy: Found matching volume %s for device %s", volume.Name, deviceName)
			return populator.LUN{
				Name:         volume.Name,
				SerialNumber: volume.Serial,
				NAA:          fmt.Sprintf("naa.%s%s", FlashProviderID, strings.ToLower(volume.Serial)),
			}, nil
		}
	}

	return populator.LUN{}, fmt.Errorf("could not find volume matching RDM device %s", deviceName)
}

// performVolumeCopy executes the volume copy operation on Pure FlashArray
func (f *FlashArrayClonner) performVolumeCopy(sourceVolumeName, targetVolumeName string, progress chan<- uint64) error {
	// Perform the copy operation using Pure FlashArray API
	err := f.restClient.CopyVolume(sourceVolumeName, targetVolumeName)
	if err != nil {
		return fmt.Errorf("Pure FlashArray CopyVolume failed: %w", err)
	}

	progress <- 100
	return nil
}

// getKubeConfig returns the Kubernetes configuration
func getKubeConfig() (*rest.Config, error) {
	// Try in-cluster config first
	cfg, err := rest.InClusterConfig()
	if err == nil {
		return cfg, nil
	}

	// Fall back to kubeconfig file
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = filepath.Join(os.Getenv("HOME"), ".kube", "config")
	}

	return clientcmd.BuildConfigFromFlags("", kubeconfig)
}

// getKubeClient returns a Kubernetes clientset
func getKubeClient() (*kubernetes.Clientset, error) {
	cfg, err := getKubeConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig: %w", err)
	}

	coreCfg := rest.CopyConfig(cfg)
	coreCfg.ContentType = "application/vnd.kubernetes.protobuf"
	return kubernetes.NewForConfig(coreCfg)
}

// OnComplete implements the OnCompleteCapable interface
// This is called after successful FADA copy to create PortworxVolumePopulator CR for PXD handoff
func (f *FlashArrayClonner) OnComplete(pvcName, pvcNamespace string) error {
	klog.Infof("Pure FlashArray: OnComplete called for PVC %s/%s", pvcNamespace, pvcName)

	// Get Kubernetes client
	clientSet, err := getKubeClient()
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// Get the FADA PVC
	fadaPVC, err := clientSet.CoreV1().PersistentVolumeClaims(pvcNamespace).Get(
		context.Background(), pvcName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get FADA PVC %s: %w", pvcName, err)
	}

	// Check if target storage class annotation is present
	targetStorageClass, hasTargetSC := fadaPVC.Annotations["forklift.konveyor.io/target-storage-class"]
	if !hasTargetSC {
		klog.Info("Pure FlashArray: No target storage class annotation found, skipping PXD PVC creation")
		return nil
	}

	klog.Infof("Pure FlashArray: Found target storage class annotation: %s, creating PXD PVC", targetStorageClass)

	// Create PXD PVC with the target storage class
	pxdPVCName, err := f.createPXDPVC(clientSet, fadaPVC, targetStorageClass, pvcNamespace)
	if err != nil {
		return fmt.Errorf("failed to create PXD PVC: %w", err)
	}

	klog.Infof("Pure FlashArray: Successfully created PXD PVC: %s", pxdPVCName)

	// Get PXD PVC to extract dataSourceRef name
	// Retry with backoff since the PVC might take time to appear in API server
	var pxdPVC *corev1.PersistentVolumeClaim
	var getPVCErr error
	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		pxdPVC, getPVCErr = clientSet.CoreV1().PersistentVolumeClaims(pvcNamespace).Get(
			context.Background(), pxdPVCName, metav1.GetOptions{})
		if getPVCErr == nil {
			klog.Infof("Pure FlashArray: Successfully retrieved PXD PVC %s", pxdPVCName)
			break
		}
		if !k8serr.IsNotFound(getPVCErr) {
			return fmt.Errorf("failed to get PXD PVC %s: %w", pxdPVCName, getPVCErr)
		}
		klog.Infof("Pure FlashArray: PXD PVC %s not found yet, retrying (%d/%d)...", pxdPVCName, i+1, maxRetries)
		// Exponential backoff: 1s, 2s, 4s, 8s, etc.
		time.Sleep(time.Duration(1<<uint(i)) * time.Second)
	}
	if getPVCErr != nil {
		return fmt.Errorf("failed to get PXD PVC %s after %d retries: %w", pxdPVCName, maxRetries, getPVCErr)
	}

	if pxdPVC.Spec.DataSourceRef == nil || pxdPVC.Spec.DataSourceRef.Name == "" {
		return fmt.Errorf("PXD PVC %s has no dataSourceRef", pxdPVCName)
	}

	populatorName := pxdPVC.Spec.DataSourceRef.Name
	klog.Infof("Pure FlashArray: Creating PortworxVolumePopulator CR: %s", populatorName)

	// Create Kubernetes config
	cfg, err := getKubeConfig()
	if err != nil {
		return fmt.Errorf("failed to build config: %w", err)
	}

	// Create dynamic client for custom resources
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %w", err)
	}

	// Define PortworxVolumePopulator GVR
	gvr := schema.GroupVersionResource{
		Group:    "forklift.konveyor.io",
		Version:  "v1beta1",
		Resource: "portworxvolumepopulators",
	}

	// Extract secret name from FADA PVC annotations
	secretName := fadaPVC.Annotations["forklift.konveyor.io/secret"]
	if secretName == "" {
		klog.Warning("Pure FlashArray: No secret annotation found on FADA PVC, using empty secretName")
	}

	// Create PortworxVolumePopulator CR
	portworxPopulator := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "forklift.konveyor.io/v1beta1",
			"kind":       "PortworxVolumePopulator",
			"metadata": map[string]interface{}{
				"name":      populatorName,
				"namespace": pvcNamespace,
				"labels": map[string]interface{}{
					"migration":    fadaPVC.Labels["migration"],
					"vmID":         fadaPVC.Labels["vmID"],
					"vmdkKey":      fadaPVC.Labels["vmdkKey"],
					"storage-type": "portworx",
				},
				"ownerReferences": []interface{}{
					map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "PersistentVolumeClaim",
						"name":       pxdPVCName,
						"uid":        string(pxdPVC.UID),
					},
				},
			},
			"spec": map[string]interface{}{
				"sourcePvc":       pvcName,
				"sourceNamespace": pvcNamespace,
				"secretName":      secretName,
			},
		},
	}

	_, err = dynClient.Resource(gvr).Namespace(pvcNamespace).Create(
		context.Background(), portworxPopulator, metav1.CreateOptions{})
	if err != nil {
		// Check if already exists
		if k8serr.IsAlreadyExists(err) {
			klog.Infof("Pure FlashArray: PortworxVolumePopulator CR already exists: %s", populatorName)
			return nil
		}
		return fmt.Errorf("failed to create PortworxVolumePopulator CR: %w", err)
	}

	klog.Infof("Pure FlashArray: Successfully created PortworxVolumePopulator CR: %s", populatorName)

	// Mark FADA PVC as needing rebinding by adding target PVC annotation
	if fadaPVC.Annotations == nil {
		fadaPVC.Annotations = make(map[string]string)
	}
	fadaPVC.Annotations["forklift.konveyor.io/target-pvc-name"] = pxdPVCName

	_, err = clientSet.CoreV1().PersistentVolumeClaims(pvcNamespace).Update(
		context.Background(), fadaPVC, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update FADA PVC annotations: %w", err)
	}

	klog.Infof("Pure FlashArray: Marked FADA PVC %s for rebinding to target PVC %s", pvcName, pxdPVCName)
	return nil
}

// createPXDPVC creates a PXD PVC with the target storage class
// It copies all labels and annotations from the FADA PVC except forklift.konveyor.io/target-storage-class
func (f *FlashArrayClonner) createPXDPVC(clientSet *kubernetes.Clientset, fadaPVC *corev1.PersistentVolumeClaim, targetStorageClass, namespace string) (string, error) {
	// Generate PXD PVC name
	pxdPVCName := "pxd-" + fadaPVC.Name

	klog.Infof("Pure FlashArray: Creating PXD PVC %s with storage class %s", pxdPVCName, targetStorageClass)

	// Copy labels from FADA PVC
	labels := make(map[string]string)
	for k, v := range fadaPVC.Labels {
		labels[k] = v
	}

	// Copy annotations from FADA PVC, excluding target-storage-class
	annotations := make(map[string]string)
	for k, v := range fadaPVC.Annotations {
		if k != "forklift.konveyor.io/target-storage-class" {
			annotations[k] = v
		}
	}

	// Get the storage size from FADA PVC
	storageSize := fadaPVC.Spec.Resources.Requests[corev1.ResourceStorage]

	// Create PXD PVC
	pxdPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        pxdPVCName,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      fadaPVC.Spec.AccessModes,
			StorageClassName: &targetStorageClass,
			VolumeMode:       fadaPVC.Spec.VolumeMode,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: storageSize,
				},
			},
			DataSourceRef: &corev1.TypedObjectReference{
				APIGroup: stringPtr("forklift.konveyor.io"),
				Kind:     "PortworxVolumePopulator",
				Name:     pxdPVCName, // Will be the name of the PortworxVolumePopulator CR
			},
		},
	}

	// Create the PXD PVC
	createdPVC, err := clientSet.CoreV1().PersistentVolumeClaims(namespace).Create(
		context.Background(), pxdPVC, metav1.CreateOptions{})
	if err != nil {
		if k8serr.IsAlreadyExists(err) {
			klog.Infof("Pure FlashArray: PXD PVC %s already exists", pxdPVCName)
			return pxdPVCName, nil
		}
		return "", fmt.Errorf("failed to create PXD PVC %s: %w", pxdPVCName, err)
	}

	klog.Infof("Pure FlashArray: Successfully created PXD PVC %s (UID: %s)", pxdPVCName, createdPVC.UID)
	return pxdPVCName, nil
}

// stringPtr returns a pointer to the given string
func stringPtr(s string) *string {
	return &s
}
