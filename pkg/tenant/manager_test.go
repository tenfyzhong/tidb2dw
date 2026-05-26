package tenant_test

import (
	"context"
	"testing"

	"github.com/pingcap-inc/tidb2dw/pkg/model"
	"github.com/pingcap-inc/tidb2dw/pkg/taskstore"
	"github.com/pingcap-inc/tidb2dw/pkg/tenant"
	"github.com/stretchr/testify/require"
)

func TestManagerCreatesTaskInsideTenantContext(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)

	manifest, err := manager.CreateTask(ctx, "tenant-a", model.CreateTaskRequest{
		TaskID: "orders-to-snowflake",
		Mode:   model.TaskModeFull,
		Source: model.TaskSource{
			TableFilter: model.TableFilter{Include: []string{"orders.*"}, CaseSensitive: false},
			Tables: []model.TableBinding{
				{
					Database:       "orders",
					Table:          "orders",
					TargetDatabase: "ANALYTICS",
					TargetSchema:   "TENANT_A",
					TargetTable:    "orders",
					ColumnFilter: model.ColumnFilter{
						Mode:    model.ColumnFilterModeInclude,
						Columns: []string{"id", "status", "updated_at"},
					},
				},
			},
		},
		Storage: model.StorageConfig{URI: "s3://company-replication/tenants/tenant-a"},
		Sink:    model.SinkConfig{Type: model.SinkTypeSnowflake, Database: "ANALYTICS", Schema: "TENANT_A"},
	})
	require.NoError(t, err)
	require.Equal(t, "tenant-a", manifest.TenantID)
	require.Equal(t, "keyspace_a", manifest.Keyspace)
	require.Equal(t, model.TaskStatusCreating, manifest.Status)

	listed, err := manager.ListTasks(ctx, "tenant-a")
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, "orders-to-snowflake", listed[0].TaskID)
}

func TestManagerRejectsTenantIdentityOverride(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)

	_, err := manager.CreateTask(ctx, "tenant-a", model.CreateTaskRequest{
		TenantID: "tenant-b",
		Keyspace: "keyspace_a",
		TaskID:   "bad-task",
		Mode:     model.TaskModeFull,
		Source: model.TaskSource{
			TableFilter: model.TableFilter{Include: []string{"orders.*"}},
			Tables: []model.TableBinding{
				{Database: "orders", Table: "orders", TargetDatabase: "ANALYTICS", TargetSchema: "TENANT_A", TargetTable: "orders"},
			},
		},
		Storage: model.StorageConfig{URI: "s3://company-replication/tenants/tenant-a"},
		Sink:    model.SinkConfig{Type: model.SinkTypeSnowflake, Database: "ANALYTICS", Schema: "TENANT_A"},
	})
	require.ErrorContains(t, err, "tenant_id")
}

func TestManagerRejectsSinkOutsideTenantAllowlist(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)

	_, err := manager.CreateTask(ctx, "tenant-a", model.CreateTaskRequest{
		TaskID: "bad-target",
		Mode:   model.TaskModeFull,
		Source: model.TaskSource{
			TableFilter: model.TableFilter{Include: []string{"orders.*"}},
			Tables: []model.TableBinding{
				{Database: "orders", Table: "orders", TargetDatabase: "ANALYTICS", TargetSchema: "TENANT_B", TargetTable: "orders"},
			},
		},
		Storage: model.StorageConfig{URI: "s3://company-replication/tenants/tenant-a"},
		Sink:    model.SinkConfig{Type: model.SinkTypeSnowflake, Database: "ANALYTICS", Schema: "TENANT_B"},
	})
	require.ErrorContains(t, err, "target schema")
}

func TestManagerRejectsTargetTableCollision(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)

	_, err := manager.CreateTask(ctx, "tenant-a", model.CreateTaskRequest{
		TaskID: "bad-collision",
		Mode:   model.TaskModeFull,
		Source: model.TaskSource{
			TableFilter: model.TableFilter{Include: []string{"orders.*"}},
			Tables: []model.TableBinding{
				{Database: "orders", Table: "orders", TargetDatabase: "ANALYTICS", TargetSchema: "TENANT_A", TargetTable: "orders"},
				{Database: "billing", Table: "orders", TargetDatabase: "ANALYTICS", TargetSchema: "TENANT_A", TargetTable: "orders"},
			},
		},
		Storage: model.StorageConfig{URI: "s3://company-replication/tenants/tenant-a"},
		Sink:    model.SinkConfig{Type: model.SinkTypeSnowflake, Database: "ANALYTICS", Schema: "TENANT_A"},
	})
	require.ErrorContains(t, err, "target table collision")
}

func newTestManager(t *testing.T) *tenant.Manager {
	t.Helper()

	manager, err := tenant.NewManager(model.TenantRegistry{
		Tenants: []model.TenantConfig{
			{
				TenantID:   "tenant-a",
				Keyspace:   "keyspace_a",
				StorageURI: "s3://company-replication/tenants/tenant-a",
				SinkPolicy: model.SinkPolicy{
					AllowedDatabases: []string{"ANALYTICS"},
					AllowedSchemas:   []string{"TENANT_A"},
				},
				Limits: model.TenantLimits{
					SnapshotConcurrency: 8,
					MaxTableWorkers:     16,
				},
			},
		},
	}, taskstore.NewMemoryStoreFactory())
	require.NoError(t, err)
	return manager
}
