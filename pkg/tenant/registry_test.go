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

func TestLoadRegistryFileResolvesSecretRefs(t *testing.T) {
	t.Setenv("TENANT_A_TOKEN", "token-a")
	t.Setenv("TENANT_A_TIDB_PASS", "tidb-pass")
	t.Setenv("TENANT_A_SF_PASS", "snowflake-pass")
	t.Setenv("TENANT_A_AWS_SECRET", "aws-secret")

	registryPath := filepath.Join(t.TempDir(), "tenants.json")
	require.NoError(t, os.WriteFile(registryPath, []byte(`{
  "tenants": [
    {
      "tenant_id": "tenant-a",
      "keyspace": "keyspace_a",
      "storage_uri": "s3://company-replication/tenants/tenant-a",
      "auth": {
        "bearer_token": "env:TENANT_A_TOKEN"
      },
      "storage_credentials": {
        "access_key_id": "ak",
        "secret_access_key": "env:TENANT_A_AWS_SECRET"
      },
      "source": {
        "tidb": {
          "pass": "env:TENANT_A_TIDB_PASS"
        }
      },
      "sink": {
        "pass": "env:TENANT_A_SF_PASS"
      }
    }
  ]
}`), 0o600))

	registry, err := tenant.LoadRegistryFile(registryPath)
	require.NoError(t, err)
	require.Equal(t, "token-a", registry.Tenants[0].Auth.BearerToken)
	require.Equal(t, "tidb-pass", registry.Tenants[0].Source.TiDB.Pass)
	require.Equal(t, "snowflake-pass", registry.Tenants[0].Sink.Pass)
	require.Equal(t, "aws-secret", registry.Tenants[0].StorageCredentials.SecretAccessKey)
}
