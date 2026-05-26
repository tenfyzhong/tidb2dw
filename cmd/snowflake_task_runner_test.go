package cmd

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
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
	require.Equal(t, "tenant-a", cfg.cdcConfig.Namespace)
	require.Equal(t, "tidb2dw-tenant-a-orders-to-snowflake", cfg.cdcConfig.ChangefeedID)

	updated, err := store.GetManifest(ctx, "orders-to-snowflake")
	require.NoError(t, err)
	require.Equal(t, model.TaskStatusRunning, updated.Status)
}

func TestSnowflakeTaskRunnerPreparesTaskByResolvingTableFilter(t *testing.T) {
	ctx := context.Background()
	runner := NewSnowflakeTaskRunner(SnowflakeTaskRunnerConfig{
		DefaultTiDB:           tidbsql.TiDBConfig{Host: "tidb", Port: 4000, User: "root"},
		DefaultCDC:            model.SourceCDCConfig{Host: "ticdc", Port: 8300},
		DefaultSnowflake:      snowsql.SnowflakeConfig{AccountId: "acct", Warehouse: "wh", User: "u", Pass: "p", Database: "ANALYTICS", Schema: "TENANT_A"},
		DefaultAWSCredentials: credentials.Value{AccessKeyID: "ak", SecretAccessKey: "sk"},
		TableListProvider: func(_ context.Context, _ tidbsql.TiDBConfig) ([]model.TableName, error) {
			return []model.TableName{
				{Database: "orders", Table: "orders"},
				{Database: "orders", Table: "tmp_202605"},
				{Database: "billing", Table: "invoices"},
			}, nil
		},
	})

	manifest, err := runner.PrepareTask(ctx, model.TenantConfig{
		TenantID:   "tenant-a",
		Keyspace:   "keyspace_a",
		StorageURI: "s3://company-replication/tenants/tenant-a",
	}, model.TaskManifest{
		Version:  1,
		TenantID: "tenant-a",
		Keyspace: "keyspace_a",
		TaskID:   "orders-to-snowflake",
		Mode:     model.TaskModeFull,
		Source: model.TaskSource{
			TableFilter: model.TableFilter{Include: []string{"orders.*"}, Exclude: []string{"orders.tmp_*"}},
		},
		Storage: model.StorageConfig{URI: "s3://company-replication/tenants/tenant-a"},
		Sink:    model.SinkConfig{Type: model.SinkTypeSnowflake, Database: "ANALYTICS", Schema: "TENANT_A"},
	})
	require.NoError(t, err)
	require.Equal(t, []model.TableBinding{
		{Database: "orders", Table: "orders", TargetDatabase: "ANALYTICS", TargetSchema: "TENANT_A", TargetTable: "orders"},
	}, manifest.Source.Tables)
	require.Equal(t, "tenant-a", manifest.Source.CDC.Namespace)
	require.Equal(t, "tidb2dw-tenant-a-orders-to-snowflake", manifest.Source.CDC.ChangefeedID)
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

func TestSnowflakeTaskRunnerResumesPausedChangefeedBeforeRestart(t *testing.T) {
	resumed := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resumed <- r.Method + " " + r.URL.String()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	parsed, err := url.Parse(server.URL)
	require.NoError(t, err)
	host, portValue, err := net.SplitHostPort(parsed.Host)
	require.NoError(t, err)
	port, err := strconv.Atoi(portValue)
	require.NoError(t, err)

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
		Status:   model.TaskStatusPaused,
		Source: model.TaskSource{
			Tables: []model.TableBinding{{Database: "orders", Table: "orders"}},
		},
		Storage: model.StorageConfig{URI: "s3://company-replication/tenants/tenant-a"},
		Sink:    model.SinkConfig{Type: model.SinkTypeSnowflake, Database: "ANALYTICS", Schema: "TENANT_A"},
	}
	require.NoError(t, store.CreateManifest(ctx, manifest))

	started := make(chan struct{}, 1)
	runner := NewSnowflakeTaskRunner(SnowflakeTaskRunnerConfig{
		DefaultTiDB:             tidbsql.TiDBConfig{Host: "tidb", Port: 4000, User: "root"},
		DefaultCDC:              model.SourceCDCConfig{Host: host, Port: port},
		DefaultSnowflake:        snowsql.SnowflakeConfig{AccountId: "acct", Warehouse: "wh", User: "u", Pass: "p", Database: "ANALYTICS", Schema: "TENANT_A"},
		DefaultAWSCredentials:   credentials.Value{AccessKeyID: "ak", SecretAccessKey: "sk"},
		DefaultCDCFlushInterval: time.Minute,
	})
	runner.runTask = func(context.Context, snowflakeTaskConfig) error {
		started <- struct{}{}
		return nil
	}

	require.NoError(t, runner.StartTask(ctx, model.TenantConfig{
		TenantID:   "tenant-a",
		Keyspace:   "keyspace_a",
		StorageURI: "s3://company-replication/tenants/tenant-a",
	}, manifest, store))

	select {
	case got := <-resumed:
		require.Equal(t, "POST /api/v2/changefeeds/tidb2dw-tenant-a-orders-to-snowflake/resume?namespace=tenant-a", got)
	case <-time.After(2 * time.Second):
		t.Fatal("changefeed was not resumed")
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("task did not start")
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
