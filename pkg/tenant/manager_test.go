package tenant_test

import (
	"context"
	"errors"
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

func TestManagerValidatesBeforePersistingAndStartingTask(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)
	runner := &fakeRunner{validateErr: errors.New("schema validation failed")}
	manager.SetTaskRunner(runner)

	_, err := manager.CreateTask(ctx, "tenant-a", validCreateTaskRequest())
	require.ErrorContains(t, err, "schema validation failed")
	require.Len(t, runner.validated, 1)
	require.Empty(t, runner.started)

	listed, listErr := manager.ListTasks(ctx, "tenant-a")
	require.NoError(t, listErr)
	require.Empty(t, listed)
}

func TestManagerAllowsRunnerToResolveTablesFromTableFilter(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)
	runner := &fakeRunner{
		prepare: func(_ context.Context, _ model.TenantConfig, manifest model.TaskManifest) (model.TaskManifest, error) {
			require.Empty(t, manifest.Source.Tables)
			manifest.Source.Tables = []model.TableBinding{
				{Database: "orders", Table: "orders", TargetDatabase: "ANALYTICS", TargetSchema: "TENANT_A", TargetTable: "orders"},
			}
			return manifest, nil
		},
	}
	manager.SetTaskRunner(runner)

	req := validCreateTaskRequest()
	req.Source.Tables = nil
	manifest, err := manager.CreateTask(ctx, "tenant-a", req)
	require.NoError(t, err)
	require.Equal(t, []model.TableBinding{
		{Database: "orders", Table: "orders", TargetDatabase: "ANALYTICS", TargetSchema: "TENANT_A", TargetTable: "orders"},
	}, manifest.Source.Tables)
	require.Len(t, runner.prepared, 1)
	require.Len(t, runner.validated, 1)
	require.Len(t, runner.started, 1)
}

func TestManagerStartsPausesAndResumesTasks(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)
	runner := &fakeRunner{}
	manager.SetTaskRunner(runner)

	manifest, err := manager.CreateTask(ctx, "tenant-a", validCreateTaskRequest())
	require.NoError(t, err)
	require.Equal(t, "orders-to-snowflake", manifest.TaskID)
	require.Len(t, runner.validated, 1)
	require.Len(t, runner.started, 1)

	_, err = manager.PauseTask(ctx, "tenant-a", "orders-to-snowflake")
	require.NoError(t, err)
	require.Equal(t, []string{"tenant-a/orders-to-snowflake"}, runner.stopped)
	require.Equal(t, []string{"tenant-a/orders-to-snowflake"}, runner.paused)

	paused, err := manager.GetTask(ctx, "tenant-a", "orders-to-snowflake")
	require.NoError(t, err)
	require.Equal(t, model.TaskStatusPaused, paused.Status)

	_, err = manager.ResumeTask(ctx, "tenant-a", "orders-to-snowflake")
	require.NoError(t, err)
	require.Len(t, runner.started, 2)

	resumed, err := manager.GetTask(ctx, "tenant-a", "orders-to-snowflake")
	require.NoError(t, err)
	require.Equal(t, model.TaskStatusRunning, resumed.Status)
}

func TestManagerDeletesTaskThroughRunnerBeforeTombstone(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)
	runner := &fakeRunner{}
	manager.SetTaskRunner(runner)

	_, err := manager.CreateTask(ctx, "tenant-a", validCreateTaskRequest())
	require.NoError(t, err)
	require.NoError(t, manager.TombstoneTask(ctx, "tenant-a", "orders-to-snowflake"))

	require.Equal(t, []string{"tenant-a/orders-to-snowflake"}, runner.deleted)
	_, err = manager.GetTask(ctx, "tenant-a", "orders-to-snowflake")
	require.ErrorContains(t, err, "tombstoned")
}

func TestManagerEnforcesTenantTableWorkerQuota(t *testing.T) {
	ctx := context.Background()
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
				Limits: model.TenantLimits{MaxTableWorkers: 1},
			},
		},
	}, taskstore.NewMemoryStoreFactory())
	require.NoError(t, err)

	_, err = manager.CreateTask(ctx, "tenant-a", validCreateTaskRequest())
	require.NoError(t, err)

	second := validCreateTaskRequest()
	second.TaskID = "second-task"
	_, err = manager.CreateTask(ctx, "tenant-a", second)
	require.ErrorContains(t, err, "tenant table worker quota")
}

func TestManagerStartsLoadedTasksForRecovery(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)
	manifest, err := manager.CreateTask(ctx, "tenant-a", validCreateTaskRequest())
	require.NoError(t, err)

	runner := &fakeRunner{}
	manager.SetTaskRunner(runner)

	require.NoError(t, manager.StartLoadedTasks(ctx))
	require.Len(t, runner.started, 1)
	require.Equal(t, manifest.TaskID, runner.started[0].TaskID)
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

func validCreateTaskRequest() model.CreateTaskRequest {
	return model.CreateTaskRequest{
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
				},
			},
		},
		Storage: model.StorageConfig{URI: "s3://company-replication/tenants/tenant-a"},
		Sink:    model.SinkConfig{Type: model.SinkTypeSnowflake, Database: "ANALYTICS", Schema: "TENANT_A"},
	}
}

type fakeRunner struct {
	validateErr error
	prepare     func(context.Context, model.TenantConfig, model.TaskManifest) (model.TaskManifest, error)
	prepared    []model.TaskManifest
	validated   []model.TaskManifest
	started     []model.TaskManifest
	stopped     []string
	paused      []string
	deleted     []string
}

func (r *fakeRunner) PrepareTask(ctx context.Context, tenantConfig model.TenantConfig, manifest model.TaskManifest) (model.TaskManifest, error) {
	r.prepared = append(r.prepared, manifest)
	if r.prepare != nil {
		return r.prepare(ctx, tenantConfig, manifest)
	}
	return manifest, nil
}

func (r *fakeRunner) ValidateTask(_ context.Context, _ model.TenantConfig, manifest model.TaskManifest) error {
	r.validated = append(r.validated, manifest)
	return r.validateErr
}

func (r *fakeRunner) StartTask(_ context.Context, _ model.TenantConfig, manifest model.TaskManifest, _ taskstore.Store) error {
	r.started = append(r.started, manifest)
	return nil
}

func (r *fakeRunner) StopTask(_ context.Context, tenantConfig model.TenantConfig, taskID string) error {
	r.stopped = append(r.stopped, tenantConfig.TenantID+"/"+taskID)
	return nil
}

func (r *fakeRunner) PauseTask(_ context.Context, tenantConfig model.TenantConfig, manifest model.TaskManifest) error {
	r.paused = append(r.paused, tenantConfig.TenantID+"/"+manifest.TaskID)
	return r.StopTask(context.Background(), tenantConfig, manifest.TaskID)
}

func (r *fakeRunner) DeleteTask(_ context.Context, tenantConfig model.TenantConfig, manifest model.TaskManifest) error {
	r.deleted = append(r.deleted, tenantConfig.TenantID+"/"+manifest.TaskID)
	return nil
}
