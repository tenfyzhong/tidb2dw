package tenant

import (
	"context"
	"crypto/subtle"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pingcap-inc/tidb2dw/pkg/filter"
	"github.com/pingcap-inc/tidb2dw/pkg/model"
	"github.com/pingcap-inc/tidb2dw/pkg/taskstore"
)

type Manager struct {
	tenants map[string]*Context
	runner  TaskRunner
}

type Context struct {
	Config model.TenantConfig
	store  taskstore.Store
	mu     sync.Mutex
}

type TaskRunner interface {
	ValidateTask(ctx context.Context, tenantConfig model.TenantConfig, manifest model.TaskManifest) error
	StartTask(ctx context.Context, tenantConfig model.TenantConfig, manifest model.TaskManifest, store taskstore.Store) error
	StopTask(ctx context.Context, tenantConfig model.TenantConfig, taskID string) error
}

type TaskPreparer interface {
	PrepareTask(ctx context.Context, tenantConfig model.TenantConfig, manifest model.TaskManifest) (model.TaskManifest, error)
}

type TaskLifecycleRunner interface {
	PauseTask(ctx context.Context, tenantConfig model.TenantConfig, manifest model.TaskManifest) error
	DeleteTask(ctx context.Context, tenantConfig model.TenantConfig, manifest model.TaskManifest) error
}

type Status struct {
	TenantID string             `json:"tenant_id"`
	Keyspace string             `json:"keyspace"`
	Limits   model.TenantLimits `json:"limits,omitempty"`
}

var (
	ErrUnauthorized = fmt.Errorf("missing or invalid bearer token")
	ErrForbidden    = fmt.Errorf("bearer token is not authorized for tenant")
)

func NewManager(registry model.TenantRegistry, factory taskstore.Factory) (*Manager, error) {
	if factory == nil {
		return nil, fmt.Errorf("task store factory must not be nil")
	}
	manager := &Manager{tenants: make(map[string]*Context, len(registry.Tenants))}
	for _, tenantConfig := range registry.Tenants {
		if tenantConfig.TenantID == "" {
			return nil, fmt.Errorf("tenant_id must not be empty")
		}
		if tenantConfig.Keyspace == "" {
			return nil, fmt.Errorf("tenant %q keyspace must not be empty", tenantConfig.TenantID)
		}
		if _, ok := manager.tenants[tenantConfig.TenantID]; ok {
			return nil, fmt.Errorf("duplicate tenant_id %q", tenantConfig.TenantID)
		}
		store, err := factory.NewStore(tenantConfig)
		if err != nil {
			return nil, err
		}
		manager.tenants[tenantConfig.TenantID] = &Context{
			Config: tenantConfig,
			store:  store,
		}
	}
	return manager, nil
}

func (m *Manager) SetTaskRunner(runner TaskRunner) {
	m.runner = runner
}

func (m *Manager) LoadedTenantCount() int {
	return len(m.tenants)
}

func (m *Manager) ListTenants() []Status {
	statuses := make([]Status, 0, len(m.tenants))
	for _, tenantCtx := range m.tenants {
		statuses = append(statuses, Status{
			TenantID: tenantCtx.Config.TenantID,
			Keyspace: tenantCtx.Config.Keyspace,
			Limits:   tenantCtx.Config.Limits,
		})
	}
	sort.Slice(statuses, func(i, j int) bool {
		return statuses[i].TenantID < statuses[j].TenantID
	})
	return statuses
}

func (m *Manager) GetTenant(tenantID string) (Status, error) {
	tenantCtx, err := m.getContext(tenantID)
	if err != nil {
		return Status{}, err
	}
	return Status{
		TenantID: tenantCtx.Config.TenantID,
		Keyspace: tenantCtx.Config.Keyspace,
		Limits:   tenantCtx.Config.Limits,
	}, nil
}

func (m *Manager) CreateTask(ctx context.Context, tenantID string, req model.CreateTaskRequest) (model.TaskManifest, error) {
	tenantCtx, err := m.getContext(tenantID)
	if err != nil {
		return model.TaskManifest{}, err
	}
	if err := validateTenantBoundRequest(tenantCtx.Config, req); err != nil {
		return model.TaskManifest{}, err
	}

	manifest, err := buildManifest(tenantCtx.Config, req)
	if err != nil {
		return model.TaskManifest{}, err
	}
	if preparer, ok := m.runner.(TaskPreparer); ok {
		manifest, err = preparer.PrepareTask(ctx, tenantCtx.Config, manifest)
		if err != nil {
			return model.TaskManifest{}, err
		}
	}
	if err := validateTaskManifestForTenant(tenantCtx.Config, manifest); err != nil {
		return model.TaskManifest{}, err
	}

	if m.runner != nil {
		if err := m.runner.ValidateTask(ctx, tenantCtx.Config, manifest); err != nil {
			return model.TaskManifest{}, err
		}
	}

	tenantCtx.mu.Lock()
	defer tenantCtx.mu.Unlock()

	if err := tenantCtx.ensureTableWorkerQuota(ctx, manifest, false); err != nil {
		return model.TaskManifest{}, err
	}
	if err := tenantCtx.store.CreateManifest(ctx, manifest); err != nil {
		return model.TaskManifest{}, err
	}
	if m.runner != nil {
		if err := m.runner.StartTask(ctx, tenantCtx.Config, manifest, tenantCtx.store); err != nil {
			return model.TaskManifest{}, err
		}
	}
	return manifest, nil
}

func (m *Manager) ListTasks(ctx context.Context, tenantID string) ([]model.TaskManifest, error) {
	tenantCtx, err := m.getContext(tenantID)
	if err != nil {
		return nil, err
	}
	return tenantCtx.store.ListManifests(ctx)
}

func (m *Manager) GetTask(ctx context.Context, tenantID string, taskID string) (model.TaskManifest, error) {
	tenantCtx, err := m.getContext(tenantID)
	if err != nil {
		return model.TaskManifest{}, err
	}
	return tenantCtx.store.GetManifest(ctx, taskID)
}

func (m *Manager) TombstoneTask(ctx context.Context, tenantID string, taskID string) error {
	tenantCtx, err := m.getContext(tenantID)
	if err != nil {
		return err
	}
	tenantCtx.mu.Lock()
	defer tenantCtx.mu.Unlock()
	manifest, err := tenantCtx.store.GetManifest(ctx, taskID)
	if err != nil {
		return err
	}
	if m.runner != nil {
		if lifecycle, ok := m.runner.(TaskLifecycleRunner); ok {
			if err := lifecycle.DeleteTask(ctx, tenantCtx.Config, manifest); err != nil {
				return err
			}
		} else {
			if err := m.runner.StopTask(ctx, tenantCtx.Config, taskID); err != nil {
				return err
			}
		}
	}
	return tenantCtx.store.TombstoneManifest(ctx, taskID, time.Now().UTC())
}

func (m *Manager) PauseTask(ctx context.Context, tenantID string, taskID string) (model.TaskManifest, error) {
	tenantCtx, err := m.getContext(tenantID)
	if err != nil {
		return model.TaskManifest{}, err
	}
	tenantCtx.mu.Lock()
	defer tenantCtx.mu.Unlock()

	manifest, err := tenantCtx.store.GetManifest(ctx, taskID)
	if err != nil {
		return model.TaskManifest{}, err
	}
	if m.runner != nil {
		if lifecycle, ok := m.runner.(TaskLifecycleRunner); ok {
			if err := lifecycle.PauseTask(ctx, tenantCtx.Config, manifest); err != nil {
				return model.TaskManifest{}, err
			}
		} else {
			if err := m.runner.StopTask(ctx, tenantCtx.Config, taskID); err != nil {
				return model.TaskManifest{}, err
			}
		}
	}
	manifest.Status = model.TaskStatusPaused
	manifest.UpdatedAt = time.Now().UTC()
	if err := tenantCtx.store.UpdateManifest(ctx, manifest); err != nil {
		return model.TaskManifest{}, err
	}
	return manifest, nil
}

func (m *Manager) ResumeTask(ctx context.Context, tenantID string, taskID string) (model.TaskManifest, error) {
	tenantCtx, err := m.getContext(tenantID)
	if err != nil {
		return model.TaskManifest{}, err
	}
	tenantCtx.mu.Lock()
	defer tenantCtx.mu.Unlock()

	manifest, err := tenantCtx.store.GetManifest(ctx, taskID)
	if err != nil {
		return model.TaskManifest{}, err
	}
	if err := tenantCtx.ensureTableWorkerQuota(ctx, manifest, true); err != nil {
		return model.TaskManifest{}, err
	}
	if m.runner != nil {
		if err := m.runner.StartTask(ctx, tenantCtx.Config, manifest, tenantCtx.store); err != nil {
			return model.TaskManifest{}, err
		}
	}
	manifest.Status = model.TaskStatusRunning
	manifest.UpdatedAt = time.Now().UTC()
	if err := tenantCtx.store.UpdateManifest(ctx, manifest); err != nil {
		return model.TaskManifest{}, err
	}
	return manifest, nil
}

func (m *Manager) StartLoadedTasks(ctx context.Context) error {
	if m.runner == nil {
		return nil
	}
	for _, tenantCtx := range m.tenants {
		manifests, err := tenantCtx.store.ListManifests(ctx)
		if err != nil {
			return err
		}
		for _, manifest := range manifests {
			if manifest.Status == model.TaskStatusPaused || manifest.Status == model.TaskStatusDeleted {
				continue
			}
			if err := m.runner.StartTask(ctx, tenantCtx.Config, manifest, tenantCtx.store); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Manager) AuthEnabled() bool {
	for _, tenantCtx := range m.tenants {
		if tenantCtx.Config.Auth.BearerToken != "" {
			return true
		}
	}
	return false
}

func (m *Manager) AuthorizeTenantAccess(tenantID string, token string) error {
	if !m.AuthEnabled() {
		return nil
	}
	if token == "" {
		return ErrUnauthorized
	}
	tenantCtx, err := m.getContext(tenantID)
	if err != nil {
		return err
	}
	if tenantCtx.Config.Auth.BearerToken == "" {
		return ErrForbidden
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(tenantCtx.Config.Auth.BearerToken)) != 1 {
		return ErrForbidden
	}
	return nil
}

func (m *Manager) AuthorizedTenants(token string) ([]Status, error) {
	if !m.AuthEnabled() {
		return m.ListTenants(), nil
	}
	if token == "" {
		return nil, ErrUnauthorized
	}
	statuses := make([]Status, 0, len(m.tenants))
	for _, tenantCtx := range m.tenants {
		if tenantCtx.Config.Auth.BearerToken == "" {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(token), []byte(tenantCtx.Config.Auth.BearerToken)) == 1 {
			statuses = append(statuses, Status{
				TenantID: tenantCtx.Config.TenantID,
				Keyspace: tenantCtx.Config.Keyspace,
				Limits:   tenantCtx.Config.Limits,
			})
		}
	}
	if len(statuses) == 0 {
		return nil, ErrForbidden
	}
	sort.Slice(statuses, func(i, j int) bool {
		return statuses[i].TenantID < statuses[j].TenantID
	})
	return statuses, nil
}

func (m *Manager) getContext(tenantID string) (*Context, error) {
	tenantCtx, ok := m.tenants[tenantID]
	if !ok {
		return nil, fmt.Errorf("tenant %q not found", tenantID)
	}
	return tenantCtx, nil
}

func validateTenantBoundRequest(tenantConfig model.TenantConfig, req model.CreateTaskRequest) error {
	if req.TenantID != "" && req.TenantID != tenantConfig.TenantID {
		return fmt.Errorf("request tenant_id %q does not match tenant context %q", req.TenantID, tenantConfig.TenantID)
	}
	if req.Keyspace != "" && req.Keyspace != tenantConfig.Keyspace {
		return fmt.Errorf("request keyspace %q does not match tenant context %q", req.Keyspace, tenantConfig.Keyspace)
	}
	if req.Storage.URI != "" && req.Storage.URI != tenantConfig.StorageURI {
		return fmt.Errorf("request storage uri must match tenant storage root")
	}
	if len(req.Source.TableFilter.Include) == 0 {
		return fmt.Errorf("source.table_filter.include must not be empty")
	}
	if req.Sink.Type != "" && req.Sink.Type != model.SinkTypeSnowflake {
		return fmt.Errorf("unsupported sink type %q", req.Sink.Type)
	}
	if err := validateSinkTarget(tenantConfig.SinkPolicy, req.Sink.Database, req.Sink.Schema); err != nil {
		return err
	}
	return validateTaskBindingsForTenant(tenantConfig, req.Source.Tables, req.Sink)
}

func validateTaskManifestForTenant(tenantConfig model.TenantConfig, manifest model.TaskManifest) error {
	if len(manifest.Source.Tables) == 0 {
		return fmt.Errorf("source.tables must not be empty after resolving source.table_filter")
	}
	if manifest.Sink.Type != "" && manifest.Sink.Type != model.SinkTypeSnowflake {
		return fmt.Errorf("unsupported sink type %q", manifest.Sink.Type)
	}
	return validateTaskBindingsForTenant(tenantConfig, manifest.Source.Tables, manifest.Sink)
}

func validateTaskBindingsForTenant(tenantConfig model.TenantConfig, bindings []model.TableBinding, sink model.SinkConfig) error {
	targets := make(map[string]model.TableBinding, len(bindings))
	for _, binding := range bindings {
		if binding.Database == "" || binding.Table == "" {
			return fmt.Errorf("table binding source database and table must not be empty")
		}
		if filter.IsSystemSchema(binding.Database) {
			return fmt.Errorf("table binding matched system schema %q", binding.Database)
		}
		targetDatabase := firstNonEmpty(binding.TargetDatabase, sink.Database)
		targetSchema := firstNonEmpty(binding.TargetSchema, sink.Schema)
		targetTable := firstNonEmpty(binding.TargetTable, binding.Table)
		if targetDatabase == "" {
			return fmt.Errorf("target database must not be empty")
		}
		if targetSchema == "" {
			return fmt.Errorf("target schema must not be empty")
		}
		if err := validateSinkTarget(tenantConfig.SinkPolicy, targetDatabase, targetSchema); err != nil {
			return err
		}
		targetKey := strings.ToLower(fmt.Sprintf("%s.%s.%s", targetDatabase, targetSchema, targetTable))
		if previous, ok := targets[targetKey]; ok {
			return fmt.Errorf("target table collision: %s.%s and %s.%s both map to %s",
				previous.Database, previous.Table, binding.Database, binding.Table, targetKey)
		}
		targets[targetKey] = binding
	}
	return nil
}

func (c *Context) ensureTableWorkerQuota(ctx context.Context, incoming model.TaskManifest, replacingExisting bool) error {
	quota := c.Config.Limits.MaxTableWorkers
	if quota <= 0 {
		return nil
	}
	manifests, err := c.store.ListManifests(ctx)
	if err != nil {
		return err
	}
	used := 0
	for _, manifest := range manifests {
		if replacingExisting && manifest.TaskID == incoming.TaskID {
			continue
		}
		if manifest.Status == model.TaskStatusPaused || manifest.Status == model.TaskStatusDeleted {
			continue
		}
		used += taskTableDemand(manifest)
	}
	requested := taskTableDemand(incoming)
	if used+requested > quota {
		return fmt.Errorf("tenant table worker quota exceeded: used=%d requested=%d limit=%d", used, requested, quota)
	}
	return nil
}

func taskTableDemand(manifest model.TaskManifest) int {
	if len(manifest.Source.Tables) == 0 {
		return 1
	}
	return len(manifest.Source.Tables)
}

func buildManifest(tenantConfig model.TenantConfig, req model.CreateTaskRequest) (model.TaskManifest, error) {
	taskID := strings.TrimSpace(req.TaskID)
	if taskID == "" {
		return model.TaskManifest{}, fmt.Errorf("task_id must not be empty")
	}
	mode := req.Mode
	if mode == "" {
		mode = model.TaskModeFull
	}
	if mode != model.TaskModeFull && mode != model.TaskModeSnapshotOnly && mode != model.TaskModeIncrementalOnly {
		return model.TaskManifest{}, fmt.Errorf("unsupported task mode %q", mode)
	}

	source := req.Source
	for i := range source.Tables {
		if source.Tables[i].TargetDatabase == "" {
			source.Tables[i].TargetDatabase = req.Sink.Database
		}
		if source.Tables[i].TargetSchema == "" {
			source.Tables[i].TargetSchema = req.Sink.Schema
		}
		if source.Tables[i].TargetTable == "" {
			source.Tables[i].TargetTable = source.Tables[i].Table
		}
	}

	storage := req.Storage
	if storage.URI == "" {
		storage.URI = tenantConfig.StorageURI
	}
	sink := req.Sink
	if sink.Type == "" {
		sink.Type = model.SinkTypeSnowflake
	}
	now := time.Now().UTC()
	return model.TaskManifest{
		Version:   1,
		TenantID:  tenantConfig.TenantID,
		Keyspace:  tenantConfig.Keyspace,
		TaskID:    taskID,
		Mode:      mode,
		Status:    model.TaskStatusCreating,
		Source:    source,
		Storage:   storage,
		Sink:      sink,
		Limits:    req.Limits,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

func validateSinkTarget(policy model.SinkPolicy, database string, schema string) error {
	if database != "" && len(policy.AllowedDatabases) > 0 && !containsFold(policy.AllowedDatabases, database) {
		return fmt.Errorf("target database %q is outside tenant allowlist", database)
	}
	if schema != "" && len(policy.AllowedSchemas) > 0 && !containsFold(policy.AllowedSchemas, schema) {
		return fmt.Errorf("target schema %q is outside tenant allowlist", schema)
	}
	return nil
}

func containsFold(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(value, target) {
			return true
		}
	}
	return false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
