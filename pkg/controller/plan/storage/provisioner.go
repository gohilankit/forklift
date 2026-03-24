package storage

import (
	core "k8s.io/api/core/v1"
)

// VolumeProvisioner is a generic interface for storage-backend-specific volume provisioning operations.
// Different storage backends (Pure/Portworx, NetApp, Dell, etc.) can implement this interface
// to handle their specific multi-stage volume provisioning workflows.
type VolumeProvisioner interface {
	// NeedsIntermediateVolume checks if a destination storage class requires intermediate storage.
	// For example, Pure FlashArray non-direct-access storage classes (like Portworx) need
	// direct-access storage as an intermediate step.
	// Returns true if intermediate storage is needed, false otherwise.
	NeedsIntermediateVolume(destinationStorageClass string) (bool, error)

	// GetIntermediateStorageClass returns the storage class name to use for intermediate storage.
	// For example, Pure FlashArray returns its direct-access storage class name.
	// Returns empty string if no intermediate storage is needed.
	GetIntermediateStorageClass() string

	// ProvisionAdditionalVolumes creates any additional PVCs and CRs needed for this storage backend.
	// For example, for Pure/Portworx, this creates the final PVC and PortworxXcopyVolumePopulator CR.
	// Returns error if provisioning fails.
	ProvisionAdditionalVolumes(sourcePVC *core.PersistentVolumeClaim, targetStorageClass string, diskSecretName string) error
}
