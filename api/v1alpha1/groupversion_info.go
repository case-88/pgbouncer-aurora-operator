package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	Group   = "pgbouncer-aurora.io"
	Version = "v1alpha1"
)

var GroupVersion = schema.GroupVersion{Group: Group, Version: Version}

func Kind(kind string) schema.GroupKind {
	return GroupVersion.WithKind(kind).GroupKind()
}

func Resource(resource string) schema.GroupResource {
	return GroupVersion.WithResource(resource).GroupResource()
}

var SchemeBuilder = runtime.NewSchemeBuilder(func(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion, &PgBouncerAurora{}, &PgBouncerAuroraList{})
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
})

var AddToScheme = SchemeBuilder.AddToScheme
