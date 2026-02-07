package pure

import (
	"context"
	"fmt"
	"time"

	core "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/kubev2v/forklift/pkg/apis/forklift/v1beta1/ref"
)

// VolumeRebinder handles Pure/Portworx-specific volume rebinding operations.
// This is used for the FADA -> PXD volume handoff workflow where:
// 1. Migration toolkit copies VMDK to FADA volume (intermediate storage)
// 2. PortworxVolumePopulator copies FADA to PXD volume (final storage)
// 3. VM PVCs need to be rebound from FADA to PXD volumes
//
// Implements the storage.Rebinder interface.
type VolumeRebinder struct {
	Client    client.Client
	Namespace string
}

// NeedsRebinding checks if any PVC has the target-pvc-name annotation
// indicating Pure/Portworx FADA -> PXD rebinding is needed.
func (r *VolumeRebinder) NeedsRebinding(vmRef ref.Ref, pvcs []*core.PersistentVolumeClaim) bool {
	for _, pvc := range pvcs {
		if pvc.Annotations["forklift.konveyor.io/target-pvc-name"] != "" {
			return true
		}
	}
	return false
}

// RebindVolumes is an alias for RebindCompletedVolumes to implement the storage.Rebinder interface.
func (r *VolumeRebinder) RebindVolumes(vmRef ref.Ref, migrationUID string) error {
	return r.RebindCompletedVolumes(vmRef, migrationUID)
}

// RebindCompletedVolumes performs rebinding for all PVCs that have target PVCs ready.
// This replaces source PVCs with target PVCs while keeping the same PVC names.
func (r *VolumeRebinder) RebindCompletedVolumes(vmRef ref.Ref, migrationUID string) error {
	logger := log.FromContext(context.TODO())

	// Get all PVCs for this VM
	pvcLabels := map[string]string{
		"migration": migrationUID,
		"vmID":      vmRef.ID,
	}

	pvcList := &core.PersistentVolumeClaimList{}
	err := r.Client.List(
		context.TODO(),
		pvcList,
		&client.ListOptions{
			Namespace:     r.Namespace,
			LabelSelector: labels.SelectorFromSet(pvcLabels),
		})
	if err != nil {
		return fmt.Errorf("failed to list PVCs for rebinding: %w", err)
	}

	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]

		if _, ok := pvc.Annotations["lun"]; ok {
			// skip LUNs
			continue
		}

		// Check if this PVC has already been rebound
		if pvc.Annotations["forklift.konveyor.io/rebound"] == "true" {
			logger.Info("PVC already rebound, skipping", "pvc", pvc.Name)
			continue
		}

		// Check if this PVC has a target PVC for rebinding
		targetPVCName := pvc.Annotations["forklift.konveyor.io/target-pvc-name"]
		if targetPVCName == "" {
			// No rebinding needed for this PVC
			continue
		}

		// Get the target PVC
		targetPVC := &core.PersistentVolumeClaim{}
		err = r.Client.Get(
			context.TODO(),
			client.ObjectKey{Namespace: pvc.Namespace, Name: targetPVCName},
			targetPVC)
		if err != nil {
			return fmt.Errorf("failed to get target PVC %s for rebinding: %w", targetPVCName, err)
		}

		// Verify target PVC is bound before rebinding
		if targetPVC.Status.Phase != core.ClaimBound {
			return fmt.Errorf("target PVC %s is not bound (phase: %s), cannot perform rebinding",
				targetPVCName, targetPVC.Status.Phase)
		}

		// Perform the rebinding
		logger.Info("Performing rebinding for completed volume",
			"sourcePVC", pvc.Name,
			"targetPVC", targetPVCName)

		err = r.rebindTargetVolume(pvc, targetPVC)
		if err != nil {
			return fmt.Errorf("failed to rebind target volume for PVC %s: %w", pvc.Name, err)
		}

		logger.Info("Successfully rebound volume",
			"pvc", pvc.Name,
			"targetPVC", targetPVCName)
	}

	return nil
}

// rebindTargetVolume performs the rebinding of a target PV to the original source PVC name.
// This replaces the source PVC with a target PVC while keeping the same PVC name.
// This is specific to Pure/Portworx FADA -> PXD volume handoff.
func (r *VolumeRebinder) rebindTargetVolume(sourcePVC, targetPVC *core.PersistentVolumeClaim) error {
	ctx := context.TODO()
	logger := log.FromContext(ctx)
	targetPVName := targetPVC.Spec.VolumeName

	logger.Info("Starting target volume rebinding",
		"sourcePVC", sourcePVC.Name,
		"targetPVC", targetPVC.Name,
		"targetPV", targetPVName)

	// Step 1: Set Retain policy on target PV to prevent deletion
	targetPV := &core.PersistentVolume{}
	err := r.Client.Get(ctx, client.ObjectKey{Name: targetPVName}, targetPV)
	if err != nil {
		return fmt.Errorf("failed to get target PV %s: %w", targetPVName, err)
	}

	targetPV.Spec.PersistentVolumeReclaimPolicy = core.PersistentVolumeReclaimRetain
	err = r.Client.Update(ctx, targetPV)
	if err != nil {
		return fmt.Errorf("failed to set Retain policy on target PV %s: %w", targetPVName, err)
	}
	logger.Info("Set Retain policy on target PV", "pv", targetPVName)

	// Step 2: Delete temporary target PVC
	err = r.Client.Delete(ctx, targetPVC)
	if err != nil {
		return fmt.Errorf("failed to delete temporary target PVC %s: %w", targetPVC.Name, err)
	}
	logger.Info("Deleted temporary target PVC", "pvc", targetPVC.Name)

	// Step 3: Wait for target PVC to be fully deleted
	err = r.waitForPVCDeletion(targetPVC.Name, targetPVC.Namespace, 60)
	if err != nil {
		return fmt.Errorf("timeout waiting for target PVC deletion: %w", err)
	}

	// Step 4: Remove claimRef from target PV to make it Available
	targetPV.Spec.ClaimRef = nil
	err = r.Client.Update(ctx, targetPV)
	if err != nil {
		return fmt.Errorf("failed to remove claimRef from target PV %s: %w", targetPVName, err)
	}
	logger.Info("Removed claimRef from target PV", "pv", targetPVName)

	// Step 5: Wait for target PV to become Available
	err = r.waitForPVAvailable(targetPVName, 60)
	if err != nil {
		return fmt.Errorf("timeout waiting for target PV to become Available: %w", err)
	}

	// Step 6: Delete original source PVC
	err = r.Client.Delete(ctx, sourcePVC)
	if err != nil {
		return fmt.Errorf("failed to delete source PVC %s: %w", sourcePVC.Name, err)
	}
	logger.Info("Deleted source PVC", "pvc", sourcePVC.Name)

	// Step 7: Wait for source PVC to be fully deleted
	err = r.waitForPVCDeletion(sourcePVC.Name, sourcePVC.Namespace, 60)
	if err != nil {
		return fmt.Errorf("timeout waiting for source PVC deletion: %w", err)
	}

	// Step 8: Recreate PVC with original name pointing to target PV
	newPVC := &core.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sourcePVC.Name, // Use original source PVC name
			Namespace: sourcePVC.Namespace,
			Labels:    sourcePVC.Labels,
			Annotations: func() map[string]string {
				// Copy annotations but remove rebinding markers
				annotations := make(map[string]string)
				for k, v := range sourcePVC.Annotations {
					annotations[k] = v
				}
				delete(annotations, "forklift.konveyor.io/target-pvc-name")
				delete(annotations, "forklift.konveyor.io/pxd-pvc")
				// Mark as rebound for tracking
				annotations["forklift.konveyor.io/rebound"] = "true"
				return annotations
			}(),
			OwnerReferences: sourcePVC.OwnerReferences,
		},
		Spec: core.PersistentVolumeClaimSpec{
			AccessModes:      targetPVC.Spec.AccessModes,
			StorageClassName: targetPVC.Spec.StorageClassName,
			VolumeMode:       targetPVC.Spec.VolumeMode,
			VolumeName:       targetPVName, // Bind to target PV
			Resources:        targetPVC.Spec.Resources,
		},
	}

	err = r.Client.Create(ctx, newPVC)
	if err != nil {
		return fmt.Errorf("failed to recreate PVC %s: %w", sourcePVC.Name, err)
	}
	logger.Info("Recreated PVC with original name bound to target PV",
		"pvc", sourcePVC.Name,
		"pv", targetPVName)

	// Step 9: Wait for new PVC to bind
	err = r.waitForPVCBound(sourcePVC.Name, sourcePVC.Namespace, 60)
	if err != nil {
		return fmt.Errorf("timeout waiting for rebound PVC to bind: %w", err)
	}

	logger.Info("Successfully rebound target volume",
		"pvc", sourcePVC.Name,
		"pv", targetPVName)

	return nil
}

// waitForPVCDeletion waits for a PVC to be fully deleted
func (r *VolumeRebinder) waitForPVCDeletion(pvcName, namespace string, timeoutSeconds int) error {
	for i := 0; i < timeoutSeconds; i++ {
		pvc := &core.PersistentVolumeClaim{}
		err := r.Client.Get(
			context.TODO(),
			client.ObjectKey{Namespace: namespace, Name: pvcName},
			pvc)
		if k8serr.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("timeout waiting for PVC %s to be deleted", pvcName)
}

// waitForPVAvailable waits for a PV to become Available
func (r *VolumeRebinder) waitForPVAvailable(pvName string, timeoutSeconds int) error {
	for i := 0; i < timeoutSeconds; i++ {
		pv := &core.PersistentVolume{}
		err := r.Client.Get(
			context.TODO(),
			client.ObjectKey{Name: pvName},
			pv)
		if err != nil {
			return err
		}
		if pv.Status.Phase == core.VolumeAvailable {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("timeout waiting for PV %s to become Available", pvName)
}

// waitForPVCBound waits for a PVC to become Bound
func (r *VolumeRebinder) waitForPVCBound(pvcName, namespace string, timeoutSeconds int) error {
	for i := 0; i < timeoutSeconds; i++ {
		pvc := &core.PersistentVolumeClaim{}
		err := r.Client.Get(
			context.TODO(),
			client.ObjectKey{Namespace: namespace, Name: pvcName},
			pvc)
		if err != nil {
			return err
		}
		if pvc.Status.Phase == core.ClaimBound {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("timeout waiting for PVC %s to become Bound", pvcName)
}
