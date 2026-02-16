package v1beta1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var PortworxVolumePopulatorKind = "PortworxVolumePopulator"
var PortworxVolumePopulatorResource = "portworxvolumepopulators"

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=true
// +kubebuilder:resource:shortName={pxvp,pxvps}
type PortworxVolumePopulator struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty"`

	Spec PortworxVolumePopulatorSpec `json:"spec"`
	// +optional
	Status PortworxVolumePopulatorStatus `json:"status"`
}

type PortworxVolumePopulatorSpec struct {
	// SourcePvc is the name of the source PVC
	SourcePvc string `json:"sourcePvc"`
	// SourceNamespace is the namespace of the source PVC
	SourceNamespace string `json:"sourceNamespace"`
	// SecretName is the secret containing FlashArray credentials
	SecretName string `json:"secretName"`
}

type PortworxVolumePopulatorStatus struct {
	// +optional
	Progress string `json:"progress"`
	// +optional
	Phase string `json:"phase"`
	// +optional
	Message string `json:"message"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type PortworxVolumePopulatorList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty"`
	Items         []PortworxVolumePopulator `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PortworxVolumePopulator{}, &PortworxVolumePopulatorList{})
}
