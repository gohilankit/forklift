package storage

import (
	core "k8s.io/api/core/v1"
)

// VolumeProvisioner is a generic interface for storage-backend-specific volume provisioning operations.
// Different storage backends (Pure/Portworx, NetApp, Dell, etc.) can implement this interface
// to handle their specific multi-stage volume provisioning workflows.
type VolumeProvisioner interface {
	// NeedsAdditionalVolumes checks if any PVC requires additional volumes for this storage backend.
	// For example, Pure/Portworx needs both FADA (intermediate) and PXD (final) volumes.
	// Returns true if additional volumes are needed, false otherwise.
	NeedsAdditionalVolumes(pvc *core.PersistentVolumeClaim, storageClass string) bool

	// EnsureServiceAccount creates any storage-specific ServiceAccounts with required permissions.
	// For example, portworx-populator needs a ServiceAccount with access to portworx SCC.
	// Returns error if ServiceAccount creation fails.
	EnsureServiceAccount(namespace string) error

	// ProvisionAdditionalVolumes creates any additional PVCs and CRs needed for this storage backend.
	// For example, for Pure/Portworx, this creates the PXD PVC and PortworxVolumePopulator CR.
	// Returns error if provisioning fails.
	ProvisionAdditionalVolumes(sourcePVC *core.PersistentVolumeClaim, targetStorageClass string, diskSecretName string) error
}
