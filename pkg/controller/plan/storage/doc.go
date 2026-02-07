// Package storage provides storage-backend-specific handlers for migration operations.
// Different storage backends (Pure/Portworx, NetApp, Dell, etc.) can implement
// the Rebinder interface to handle their specific volume rebinding workflows.
package storage

import (
	"github.com/kubev2v/forklift/pkg/apis/forklift/v1beta1"
	plancontext "github.com/kubev2v/forklift/pkg/controller/plan/context"
	"github.com/kubev2v/forklift/pkg/controller/plan/storage/pure"
)

// NewRebinder creates a storage-backend-specific rebinder based on the plan context.
// Returns nil if no rebinding is needed for the current storage backend.
func NewRebinder(ctx *plancontext.Context) (Rebinder, error) {
	// Check if any storage mapping uses an offload plugin
	if ctx.Plan.Map.Storage.Name != "" {
		storageMap := ctx.Map.Storage
		if storageMap != nil {
			for _, mapping := range storageMap.Spec.Map {
				if mapping.OffloadPlugin != nil && mapping.OffloadPlugin.VSphereXcopyPluginConfig != nil {
					// Detect storage backend type and return appropriate rebinder
					switch mapping.OffloadPlugin.VSphereXcopyPluginConfig.StorageVendorProduct {
					case v1beta1.StorageVendorProductPureFlashArray:
						// Pure FlashArray
						return &pure.VolumeRebinder{
							Client:    ctx.Destination.Client,
							Namespace: ctx.Plan.Spec.TargetNamespace,
						}, nil
					// Add cases for other storage backends here as they are implemented
					default:
						// Unknown or unsupported storage backend for rebinding
						// Continue checking other mappings
						continue
					}
				}
			}
		}
	}

	// No rebinding needed
	return nil, nil
}
