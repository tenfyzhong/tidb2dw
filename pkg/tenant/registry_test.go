package tenant_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap-inc/tidb2dw/pkg/tenant"
	"github.com/stretchr/testify/require"
)

func TestLoadRegistryFile(t *testing.T) {
	registryPath := filepath.Join(t.TempDir(), "tenants.json")
	require.NoError(t, os.WriteFile(registryPath, []byte(`{
  "tenants": [
    {
      "tenant_id": "tenant-a",
      "keyspace": "keyspace_a",
      "storage_uri": "s3://company-replication/tenants/tenant-a",
      "sink_policy": {
        "allowed_databases": ["ANALYTICS"],
        "allowed_schemas": ["TENANT_A"]
      }
    }
  ]
}`), 0o600))

	registry, err := tenant.LoadRegistryFile(registryPath)
	require.NoError(t, err)
	require.Len(t, registry.Tenants, 1)
	require.Equal(t, "tenant-a", registry.Tenants[0].TenantID)
	require.Equal(t, "keyspace_a", registry.Tenants[0].Keyspace)
	require.Equal(t, []string{"TENANT_A"}, registry.Tenants[0].SinkPolicy.AllowedSchemas)
}
