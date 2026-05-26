package taskstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap-inc/tidb2dw/pkg/model"
	"github.com/pingcap-inc/tidb2dw/pkg/taskstore"
	"github.com/stretchr/testify/require"
)

func TestMemoryStoreCreatesListsGetsAndTombstonesTenantManifest(t *testing.T) {
	ctx := context.Background()
	store := taskstore.NewMemoryStore(model.TenantConfig{
		TenantID:   "tenant-a",
		Keyspace:   "keyspace_a",
		StorageURI: "s3://company-replication/tenants/tenant-a",
	})
	now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
	manifest := model.TaskManifest{
		Version:   1,
		TenantID:  "tenant-a",
		Keyspace:  "keyspace_a",
		TaskID:    "orders-to-snowflake",
		Mode:      model.TaskModeFull,
		Status:    model.TaskStatusCreating,
		Storage:   model.StorageConfig{URI: "s3://company-replication/tenants/tenant-a"},
		Sink:      model.SinkConfig{Type: model.SinkTypeSnowflake, Database: "ANALYTICS", Schema: "TENANT_A"},
		CreatedAt: now,
		UpdatedAt: now,
	}

	require.NoError(t, store.CreateManifest(ctx, manifest))

	got, err := store.GetManifest(ctx, "orders-to-snowflake")
	require.NoError(t, err)
	require.Equal(t, manifest.TaskID, got.TaskID)
	require.Equal(t, manifest.TenantID, got.TenantID)

	listed, err := store.ListManifests(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, manifest.TaskID, listed[0].TaskID)

	require.NoError(t, store.TombstoneManifest(ctx, "orders-to-snowflake", now.Add(time.Minute)))
	listed, err = store.ListManifests(ctx)
	require.NoError(t, err)
	require.Empty(t, listed)

	_, err = store.GetManifest(ctx, "orders-to-snowflake")
	require.ErrorContains(t, err, "tombstoned")
}

func TestMemoryStoreRejectsDuplicateAndWrongTenantManifest(t *testing.T) {
	ctx := context.Background()
	store := taskstore.NewMemoryStore(model.TenantConfig{
		TenantID:   "tenant-a",
		Keyspace:   "keyspace_a",
		StorageURI: "s3://company-replication/tenants/tenant-a",
	})
	manifest := model.TaskManifest{
		Version:  1,
		TenantID: "tenant-a",
		Keyspace: "keyspace_a",
		TaskID:   "orders-to-snowflake",
		Mode:     model.TaskModeFull,
		Status:   model.TaskStatusCreating,
		Storage:  model.StorageConfig{URI: "s3://company-replication/tenants/tenant-a"},
		Sink:     model.SinkConfig{Type: model.SinkTypeSnowflake, Database: "ANALYTICS", Schema: "TENANT_A"},
	}

	require.NoError(t, store.CreateManifest(ctx, manifest))
	require.ErrorContains(t, store.CreateManifest(ctx, manifest), "already exists")

	manifest.TaskID = "wrong-tenant"
	manifest.TenantID = "tenant-b"
	require.ErrorContains(t, store.CreateManifest(ctx, manifest), "tenant_id")
}

func TestLayoutBuildsTenantScopedPaths(t *testing.T) {
	layout, err := taskstore.NewLayout(model.TenantConfig{
		TenantID:   "tenant-a",
		StorageURI: "s3://company-replication/tenants/tenant-a",
	})
	require.NoError(t, err)

	require.Equal(t, "metadata/tasks/orders.json", layout.TaskManifestPath("orders"))
	require.Equal(t, "metadata/tombstones/orders.json", layout.TombstonePath("orders"))
	require.Equal(t, "runtime/tasks/orders/state.json", layout.RuntimeStatePath("orders"))
	require.Equal(t, "data/tasks/orders/snapshot/orders.orders.loadinfo", layout.SnapshotLoadInfoPath("orders", model.TableName{
		Database: "orders",
		Table:    "orders",
	}))
}

func TestLayoutRejectsStorageRootOutsideTenant(t *testing.T) {
	_, err := taskstore.NewLayout(model.TenantConfig{
		TenantID:   "tenant-a",
		StorageURI: "s3://company-replication/shared",
	})
	require.ErrorContains(t, err, "tenant_id")
}
