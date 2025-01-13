// Code generated by mdatagen. DO NOT EDIT.

package metadata

import (
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

func TestResourceAttributesConfig(t *testing.T) {
	tests := []struct {
		name string
		want ResourceAttributesConfig
	}{
		{
			name: "default",
			want: DefaultResourceAttributesConfig(),
		},
		{
			name: "all_set",
			want: ResourceAttributesConfig{
				ContainerID:               ResourceAttributeConfig{Enabled: true},
				ContainerImageName:        ResourceAttributeConfig{Enabled: true},
				ContainerImageRepoDigests: ResourceAttributeConfig{Enabled: true},
				ContainerImageTag:         ResourceAttributeConfig{Enabled: true},
				K8sClusterUID:             ResourceAttributeConfig{Enabled: true},
				K8sContainerName:          ResourceAttributeConfig{Enabled: true},
				K8sCronjobName:            ResourceAttributeConfig{Enabled: true},
				K8sDaemonsetName:          ResourceAttributeConfig{Enabled: true},
				K8sDaemonsetUID:           ResourceAttributeConfig{Enabled: true},
				K8sDeploymentName:         ResourceAttributeConfig{Enabled: true},
				K8sDeploymentUID:          ResourceAttributeConfig{Enabled: true},
				K8sJobName:                ResourceAttributeConfig{Enabled: true},
				K8sJobUID:                 ResourceAttributeConfig{Enabled: true},
				K8sNamespaceName:          ResourceAttributeConfig{Enabled: true},
				K8sNodeName:               ResourceAttributeConfig{Enabled: true},
				K8sNodeUID:                ResourceAttributeConfig{Enabled: true},
				K8sPodHostname:            ResourceAttributeConfig{Enabled: true},
				K8sPodIP:                  ResourceAttributeConfig{Enabled: true},
				K8sPodName:                ResourceAttributeConfig{Enabled: true},
				K8sPodStartTime:           ResourceAttributeConfig{Enabled: true},
				K8sPodUID:                 ResourceAttributeConfig{Enabled: true},
				K8sReplicasetName:         ResourceAttributeConfig{Enabled: true},
				K8sReplicasetUID:          ResourceAttributeConfig{Enabled: true},
				K8sStatefulsetName:        ResourceAttributeConfig{Enabled: true},
				K8sStatefulsetUID:         ResourceAttributeConfig{Enabled: true},
			},
		},
		{
			name: "none_set",
			want: ResourceAttributesConfig{
				ContainerID:               ResourceAttributeConfig{Enabled: false},
				ContainerImageName:        ResourceAttributeConfig{Enabled: false},
				ContainerImageRepoDigests: ResourceAttributeConfig{Enabled: false},
				ContainerImageTag:         ResourceAttributeConfig{Enabled: false},
				K8sClusterUID:             ResourceAttributeConfig{Enabled: false},
				K8sContainerName:          ResourceAttributeConfig{Enabled: false},
				K8sCronjobName:            ResourceAttributeConfig{Enabled: false},
				K8sDaemonsetName:          ResourceAttributeConfig{Enabled: false},
				K8sDaemonsetUID:           ResourceAttributeConfig{Enabled: false},
				K8sDeploymentName:         ResourceAttributeConfig{Enabled: false},
				K8sDeploymentUID:          ResourceAttributeConfig{Enabled: false},
				K8sJobName:                ResourceAttributeConfig{Enabled: false},
				K8sJobUID:                 ResourceAttributeConfig{Enabled: false},
				K8sNamespaceName:          ResourceAttributeConfig{Enabled: false},
				K8sNodeName:               ResourceAttributeConfig{Enabled: false},
				K8sNodeUID:                ResourceAttributeConfig{Enabled: false},
				K8sPodHostname:            ResourceAttributeConfig{Enabled: false},
				K8sPodIP:                  ResourceAttributeConfig{Enabled: false},
				K8sPodName:                ResourceAttributeConfig{Enabled: false},
				K8sPodStartTime:           ResourceAttributeConfig{Enabled: false},
				K8sPodUID:                 ResourceAttributeConfig{Enabled: false},
				K8sReplicasetName:         ResourceAttributeConfig{Enabled: false},
				K8sReplicasetUID:          ResourceAttributeConfig{Enabled: false},
				K8sStatefulsetName:        ResourceAttributeConfig{Enabled: false},
				K8sStatefulsetUID:         ResourceAttributeConfig{Enabled: false},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := loadResourceAttributesConfig(t, tt.name)
			diff := cmp.Diff(tt.want, cfg, cmpopts.IgnoreUnexported(ResourceAttributeConfig{}))
			require.Emptyf(t, diff, "Config mismatch (-expected +actual):\n%s", diff)
		})
	}
}

func loadResourceAttributesConfig(t *testing.T, name string) ResourceAttributesConfig {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)
	sub, err := cm.Sub(name)
	require.NoError(t, err)
	sub, err = sub.Sub("resource_attributes")
	require.NoError(t, err)
	cfg := DefaultResourceAttributesConfig()
	require.NoError(t, sub.Unmarshal(&cfg))
	return cfg
}
