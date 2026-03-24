package v1beta1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var PortworxXcopyVolumePopulatorKind = "PortworxXcopyVolumePopulator"
var PortworxXcopyVolumePopulatorResource = "portworxxcopyvolumepopulators"

// PortworxXcopyVolumePopulatorSpec defines the desired state of PortworxXcopyVolumePopulator
type PortworxXcopyVolumePopulatorSpec struct {
	// SourcePvc is the name of the source FADA PVC
	SourcePvc string `json:"sourcePvc"`
	// SourceNamespace is the namespace of the source FADA PVC
	SourceNamespace string `json:"sourceNamespace"`
	// SecretName is the secret containing FlashArray credentials
	SecretName string `json:"secretName"`
}

// PortworxXcopyVolumePopulatorStatus defines the observed state of PortworxXcopyVolumePopulator
type PortworxXcopyVolumePopulatorStatus struct {
	// +optional
	Progress string `json:"progress"`
	// +optional
	Phase string `json:"phase"`
	// +optional
	Message string `json:"message"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=true
// +kubebuilder:resource:shortName={pxvp,pxvps}

// PortworxXcopyVolumePopulator is the Schema for the portworxxcopyvolumepopulators API
type PortworxXcopyVolumePopulator struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`

	Spec PortworxXcopyVolumePopulatorSpec `json:"spec"`
	// +optional
	Status PortworxXcopyVolumePopulatorStatus `json:"status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PortworxXcopyVolumePopulatorList is a list of PortworxXcopyVolumePopulator resources
type PortworxXcopyVolumePopulatorList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`
	Items         []PortworxXcopyVolumePopulator `json:"items"`
}
