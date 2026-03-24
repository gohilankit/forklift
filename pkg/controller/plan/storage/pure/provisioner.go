package pure

import (
	"context"
	"fmt"

	pxapi "github.com/portworx/apis/stork/v1beta1"
	core "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// DefaultIntermediateStorageClass is the default storage class for Pure FlashArray Direct Access.
	// This is used as intermediate storage when the final destination requires a two-stage copy.
	DefaultIntermediateStorageClass = "fada"
)

// VolumeProvisioner handles Pure/Portworx-specific volume provisioning operations.
// This is used for a two-stage volume handoff workflow where:
// 1. Migration toolkit copies VMDK to intermediate volume (direct-access storage)
// 2. PortworxXcopyVolumePopulator copies intermediate to final volume (target storage)
// 3. VM PVCs need to be rebound from intermediate to final volumes
//
// Implements the storage.VolumeProvisioner interface.
type VolumeProvisioner struct {
	Client    client.Client
	Namespace string
}

// NeedsIntermediateVolume checks if a destination storage class requires intermediate storage.
// For Pure FlashArray, non-direct-access storage classes (like Portworx) need direct-access
// storage as an intermediate step for optimized data transfer.
func (p *VolumeProvisioner) NeedsIntermediateVolume(destinationStorageClass string) (bool, error) {
	isFADA, err := p.isFADAStorageClass(destinationStorageClass)
	if err != nil {
		return false, err
	}
	return !isFADA, nil
}

// GetIntermediateStorageClass returns the storage class name to use for intermediate storage.
// For Pure FlashArray, this returns the direct-access storage class.
func (p *VolumeProvisioner) GetIntermediateStorageClass() string {
	return DefaultIntermediateStorageClass
}

// isFADAStorageClass checks if a storage class is a Pure FlashArray Direct Access (FADA) storage class.
// FADA storage classes have backend="pure_block" or backend="pure_file" in their parameters.
// Returns true if the storage class is FADA, false otherwise.
func (p *VolumeProvisioner) isFADAStorageClass(storageClassName string) (bool, error) {
	ctx := context.TODO()

	// Get the StorageClass object
	storageClass := &storagev1.StorageClass{}
	err := p.Client.Get(ctx, client.ObjectKey{
		Name: storageClassName,
	}, storageClass)
	if err != nil {
		return false, fmt.Errorf("failed to get StorageClass %s: %w", storageClassName, err)
	}

	// Check if the backend parameter indicates FADA storage
	if storageClass.Parameters != nil {
		backend, exists := storageClass.Parameters["backend"]
		if exists && (backend == "pure_block" || backend == "pure_file") {
			// This IS a FADA storage class
			return true, nil
		}
	}

	// This is NOT a FADA storage class
	return false, nil
}

// ProvisionAdditionalVolumes creates the final PVC and PortworxXcopyVolumePopulator CR
// for Pure/Portworx intermediate -> final volume handoff workflow.
func (p *VolumeProvisioner) ProvisionAdditionalVolumes(sourcePVC *core.PersistentVolumeClaim, targetStorageClass string, diskSecretName string) error {
	ctx := context.TODO()
	logger := log.FromContext(ctx)

	finalPVCName := "pxd-" + sourcePVC.Name

	// Get the storage size from the source PVC
	storageSize := sourcePVC.Spec.Resources.Requests[core.ResourceStorage]

	// Create final PVC
	pvblock := core.PersistentVolumeBlock
	finalPVC := core.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        finalPVCName,
			Namespace:   sourcePVC.Namespace,
			Labels:      sourcePVC.Labels,
			Annotations: make(map[string]string),
		},
		Spec: core.PersistentVolumeClaimSpec{
			StorageClassName: &targetStorageClass,
			VolumeMode:       &pvblock,
			AccessModes:      sourcePVC.Spec.AccessModes,
			Resources: core.VolumeResourceRequirements{
				Requests: core.ResourceList{
					core.ResourceStorage: *resource.NewQuantity(storageSize.Value(), resource.BinarySI),
				},
			},
			DataSourceRef: &core.TypedObjectReference{
				APIGroup: &pxapi.SchemeGroupVersion.Group,
				Kind:     pxapi.PortworxXcopyVolumePopulatorKind,
				Name:     finalPVCName, // Will match the PortworxXcopyVolumePopulator CR name
			},
		},
	}

	// Copy annotations from intermediate PVC
	for k, v := range sourcePVC.Annotations {
		finalPVC.Annotations[k] = v
	}

	// Create final PVC
	logger.Info("Creating final PVC for Portworx handoff", "finalPVC", finalPVCName, "targetStorageClass", targetStorageClass)
	err := p.Client.Create(ctx, &finalPVC, &client.CreateOptions{})
	if err != nil {
		if k8serr.IsAlreadyExists(err) {
			logger.Info("Final PVC already exists in Kubernetes, skipping", "pvcName", finalPVCName)
		} else {
			return fmt.Errorf("failed to create final PVC: %w", err)
		}
	}

	// Fetch the final PVC to get its UID
	createdFinalPVC := &core.PersistentVolumeClaim{}
	err = p.Client.Get(ctx, client.ObjectKey{
		Namespace: finalPVC.Namespace,
		Name:      finalPVC.Name,
	}, createdFinalPVC)
	if err != nil {
		return fmt.Errorf("failed to get created final PVC: %w", err)
	}

	// Create PortworxXcopyVolumePopulator CR
	portworxPopulator := pxapi.PortworxXcopyVolumePopulator{
		ObjectMeta: metav1.ObjectMeta{
			Name:      finalPVCName,
			Namespace: sourcePVC.Namespace,
			Labels:    sourcePVC.Labels,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "PersistentVolumeClaim",
					Name:       createdFinalPVC.Name,
					UID:        createdFinalPVC.UID,
				},
			},
		},
		Spec: pxapi.PortworxXcopyVolumePopulatorSpec{
			SourcePvc:       sourcePVC.Name, // Intermediate PVC name
			SourceNamespace: sourcePVC.Namespace,
			SecretName:      diskSecretName,
		},
	}

	logger.Info("Creating PortworxXcopyVolumePopulator CR", "name", finalPVCName, "sourcePvc", sourcePVC.Name)
	err = p.Client.Create(ctx, &portworxPopulator, &client.CreateOptions{})
	if err != nil {
		if k8serr.IsAlreadyExists(err) {
			logger.Info("PortworxXcopyVolumePopulator CR already exists, skipping", "name", finalPVCName)
		} else {
			return fmt.Errorf("failed to create PortworxXcopyVolumePopulator CR: %w", err)
		}
	}

	// Add annotation to intermediate PVC for rebinding
	// Fetch the latest version of the PVC to avoid conflicts
	latestIntermediatePVC := &core.PersistentVolumeClaim{}
	err = p.Client.Get(ctx, client.ObjectKey{
		Namespace: sourcePVC.Namespace,
		Name:      sourcePVC.Name,
	}, latestIntermediatePVC)
	if err != nil {
		return fmt.Errorf("failed to get latest intermediate PVC: %w", err)
	}

	if latestIntermediatePVC.Annotations == nil {
		latestIntermediatePVC.Annotations = make(map[string]string)
	}
	latestIntermediatePVC.Annotations["forklift.konveyor.io/target-pvc-name"] = finalPVCName
	err = p.Client.Update(ctx, latestIntermediatePVC)
	if err != nil {
		return fmt.Errorf("failed to update intermediate PVC with target-pvc-name annotation: %w", err)
	}

	logger.Info("Created final PVC and PortworxXcopyVolumePopulator CR for Portworx handoff",
		"intermediatePVC", sourcePVC.Name,
		"finalPVC", finalPVCName,
		"targetStorageClass", targetStorageClass)

	return nil
}
