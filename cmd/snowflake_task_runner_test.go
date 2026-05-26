package cmd

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/model"
	"github.com/pingcap-inc/tidb2dw/pkg/snowsql"
	"github.com/pingcap-inc/tidb2dw/pkg/taskstore"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/stretchr/testify/require"
)

func TestSnowflakeTaskRunnerStartsTaskWithTaskScopedStorageAndBindings(t *testing.T) {
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
		Source: model.TaskSource{
			Tables: []model.TableBinding{
				{
					Database:       "orders",
					Table:          "orders",
					TargetDatabase: "ANALYTICS",
					TargetSchema:   "TENANT_A",
					TargetTable:    "orders_target",
					ColumnFilter: model.ColumnFilter{
						Mode:    model.ColumnFilterModeInclude,
						Columns: []string{"id", "status"},
					},
				},
			},
		},
		Storage: model.StorageConfig{URI: "s3://company-replication/tenants/tenant-a"},
		Sink:    model.SinkConfig{Type: model.SinkTypeSnowflake, Database: "ANALYTICS", Schema: "TENANT_A"},
	}
	require.NoError(t, store.CreateManifest(ctx, manifest))

	started := make(chan snowflakeTaskConfig, 1)
	runner := NewSnowflakeTaskRunner(SnowflakeTaskRunnerConfig{
		DefaultTiDB: tidbsql.TiDBConfig{Host: "tidb", Port: 4000, User: "root"},
		DefaultCDC:  model.SourceCDCConfig{Host: "ticdc", Port: 8300},
		DefaultSnowflake: snowsql.SnowflakeConfig{
			AccountId: "org-account",
			Warehouse: "COMPUTE_WH",
			User:      "sf_user",
			Pass:      "sf_pass",
			Database:  "ANALYTICS",
			Schema:    "TENANT_A",
		},
		DefaultAWSCredentials: credentials.Value{AccessKeyID: "ak", SecretAccessKey: "sk"},
	})
	runner.runTask = func(_ context.Context, cfg snowflakeTaskConfig) error {
		started <- cfg
		return nil
	}

	require.NoError(t, runner.StartTask(ctx, model.TenantConfig{
		TenantID:   "tenant-a",
		Keyspace:   "keyspace_a",
		StorageURI: "s3://company-replication/tenants/tenant-a",
	}, manifest, store))

	var cfg snowflakeTaskConfig
	select {
	case cfg = <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("task did not start")
	}

	require.Equal(t, []string{"orders.orders"}, cfg.tables)
	require.Equal(t, "/tenants/tenant-a/data/tasks/orders-to-snowflake", cfg.storageURI.Path)
	require.Equal(t, "orders_target", cfg.bindings[0].TargetTable)
	require.Equal(t, model.ColumnFilterModeInclude, cfg.bindings[0].ColumnFilter.Mode)

	updated, err := store.GetManifest(ctx, "orders-to-snowflake")
	require.NoError(t, err)
	require.Equal(t, model.TaskStatusRunning, updated.Status)
}

func TestSnowflakeTaskRunnerStopCancelsRunningTask(t *testing.T) {
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
		Source: model.TaskSource{
			Tables: []model.TableBinding{{Database: "orders", Table: "orders"}},
		},
		Storage: model.StorageConfig{URI: "s3://company-replication/tenants/tenant-a"},
		Sink:    model.SinkConfig{Type: model.SinkTypeSnowflake, Database: "ANALYTICS", Schema: "TENANT_A"},
	}
	require.NoError(t, store.CreateManifest(ctx, manifest))

	cancelled := make(chan struct{})
	runner := NewSnowflakeTaskRunner(SnowflakeTaskRunnerConfig{
		DefaultTiDB:             tidbsql.TiDBConfig{Host: "tidb", Port: 4000, User: "root"},
		DefaultCDC:              model.SourceCDCConfig{Host: "ticdc", Port: 8300},
		DefaultSnowflake:        snowsql.SnowflakeConfig{AccountId: "acct", Warehouse: "wh", User: "u", Pass: "p", Database: "ANALYTICS", Schema: "TENANT_A"},
		DefaultAWSCredentials:   credentials.Value{AccessKeyID: "ak", SecretAccessKey: "sk"},
		DefaultCDCFlushInterval: time.Minute,
	})
	runner.runTask = func(ctx context.Context, _ snowflakeTaskConfig) error {
		<-ctx.Done()
		close(cancelled)
		return ctx.Err()
	}

	require.NoError(t, runner.StartTask(ctx, model.TenantConfig{
		TenantID:   "tenant-a",
		Keyspace:   "keyspace_a",
		StorageURI: "s3://company-replication/tenants/tenant-a",
	}, manifest, store))
	require.NoError(t, runner.StopTask(ctx, model.TenantConfig{TenantID: "tenant-a"}, "orders-to-snowflake"))

	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("task was not cancelled")
	}
}

func TestTaskStorageURIUsesCredentials(t *testing.T) {
	storageURI, err := buildTaskStorageURI("s3://bucket/tenants/tenant-a", "task-1", credentials.Value{
		AccessKeyID:     "ak",
		SecretAccessKey: "sk",
		SessionToken:    "token",
	})
	require.NoError(t, err)
	require.Equal(t, "/tenants/tenant-a/data/tasks/task-1", storageURI.Path)

	query, err := url.ParseQuery(storageURI.RawQuery)
	require.NoError(t, err)
	require.Equal(t, "ak", query.Get("access-key"))
	require.Equal(t, "sk", query.Get("secret-access-key"))
	require.Equal(t, "token", query.Get("session-token"))
}
