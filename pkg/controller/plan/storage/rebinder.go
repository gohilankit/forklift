package storage

import (
	"github.com/kubev2v/forklift/pkg/apis/forklift/v1beta1/ref"
	core "k8s.io/api/core/v1"
)

// Rebinder is a generic interface for storage-backend-specific volume rebinding operations.
// Different storage backends (Pure/Portworx, NetApp, Dell, etc.) can implement this interface
// to handle their specific rebinding workflows.
type Rebinder interface {
	// NeedsRebinding checks if any PVC requires rebinding for this storage backend.
	// Returns true if rebinding is needed, false otherwise.
	NeedsRebinding(vmRef ref.Ref, pvcs []*core.PersistentVolumeClaim) bool

	// RebindVolumes performs the actual rebinding operation for completed volumes.
	// Returns error if rebinding fails.
	RebindVolumes(vmRef ref.Ref, migrationUID string) error
}
