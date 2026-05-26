package taskstore

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/pingcap-inc/tidb2dw/pkg/model"
	brstorage "github.com/pingcap/tidb/br/pkg/storage"
)

type Store interface {
	CreateManifest(ctx context.Context, manifest model.TaskManifest) error
	UpdateManifest(ctx context.Context, manifest model.TaskManifest) error
	GetManifest(ctx context.Context, taskID string) (model.TaskManifest, error)
	ListManifests(ctx context.Context) ([]model.TaskManifest, error)
	TombstoneManifest(ctx context.Context, taskID string, deletedAt time.Time) error
}

type Factory interface {
	NewStore(tenant model.TenantConfig) (Store, error)
}

type MemoryStoreFactory struct {
	mu     sync.Mutex
	stores map[string]*MemoryStore
}

type ExternalStorageOpener func(ctx context.Context, tenant model.TenantConfig) (brstorage.ExternalStorage, error)

type ExternalStoreFactory struct {
	open ExternalStorageOpener
}

func NewExternalStoreFactory(open ExternalStorageOpener) *ExternalStoreFactory {
	return &ExternalStoreFactory{open: open}
}

func (f *ExternalStoreFactory) NewStore(tenant model.TenantConfig) (Store, error) {
	if f.open == nil {
		return nil, fmt.Errorf("external storage opener must not be nil")
	}
	externalStorage, err := f.open(context.Background(), tenant)
	if err != nil {
		return nil, err
	}
	return NewExternalStore(tenant, externalStorage)
}

func NewMemoryStoreFactory() *MemoryStoreFactory {
	return &MemoryStoreFactory{stores: make(map[string]*MemoryStore)}
}

func (f *MemoryStoreFactory) NewStore(tenant model.TenantConfig) (Store, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if store, ok := f.stores[tenant.TenantID]; ok {
		return store, nil
	}
	store := NewMemoryStore(tenant)
	if store.err != nil {
		return nil, store.err
	}
	f.stores[tenant.TenantID] = store
	return store, nil
}

type MemoryStore struct {
	tenant     model.TenantConfig
	layout     *Layout
	err        error
	manifests  map[string]model.TaskManifest
	tombstones map[string]Tombstone
	mu         sync.Mutex
}

type Tombstone struct {
	TenantID  string     `json:"tenant_id"`
	Keyspace  string     `json:"keyspace"`
	TaskID    string     `json:"task_id"`
	DeletedAt model.Time `json:"deleted_at"`
}

func NewMemoryStore(tenant model.TenantConfig) *MemoryStore {
	layout, err := NewLayout(tenant)
	return &MemoryStore{
		tenant:     tenant,
		layout:     layout,
		err:        err,
		manifests:  make(map[string]model.TaskManifest),
		tombstones: make(map[string]Tombstone),
	}
}

func (s *MemoryStore) CreateManifest(_ context.Context, manifest model.TaskManifest) error {
	if err := s.ready(); err != nil {
		return err
	}
	if err := s.validateManifest(manifest); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.manifests[manifest.TaskID]; ok {
		return fmt.Errorf("task manifest %q already exists", manifest.TaskID)
	}
	if _, ok := s.tombstones[manifest.TaskID]; ok {
		return fmt.Errorf("task manifest %q is tombstoned", manifest.TaskID)
	}
	s.manifests[manifest.TaskID] = manifest
	return nil
}

func (s *MemoryStore) UpdateManifest(_ context.Context, manifest model.TaskManifest) error {
	if err := s.ready(); err != nil {
		return err
	}
	if err := s.validateManifest(manifest); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.tombstones[manifest.TaskID]; ok {
		return fmt.Errorf("task manifest %q is tombstoned", manifest.TaskID)
	}
	s.manifests[manifest.TaskID] = manifest
	return nil
}

func (s *MemoryStore) GetManifest(_ context.Context, taskID string) (model.TaskManifest, error) {
	if err := s.ready(); err != nil {
		return model.TaskManifest{}, err
	}
	if err := validateTaskID(taskID); err != nil {
		return model.TaskManifest{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.tombstones[taskID]; ok {
		return model.TaskManifest{}, fmt.Errorf("task manifest %q is tombstoned", taskID)
	}
	manifest, ok := s.manifests[taskID]
	if !ok {
		return model.TaskManifest{}, fmt.Errorf("task manifest %q not found", taskID)
	}
	return manifest, nil
}

func (s *MemoryStore) ListManifests(_ context.Context) ([]model.TaskManifest, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	taskIDs := make([]string, 0, len(s.manifests))
	for taskID := range s.manifests {
		if _, ok := s.tombstones[taskID]; !ok {
			taskIDs = append(taskIDs, taskID)
		}
	}
	sort.Strings(taskIDs)

	manifests := make([]model.TaskManifest, 0, len(taskIDs))
	for _, taskID := range taskIDs {
		manifests = append(manifests, s.manifests[taskID])
	}
	return manifests, nil
}

func (s *MemoryStore) TombstoneManifest(_ context.Context, taskID string, deletedAt time.Time) error {
	if err := s.ready(); err != nil {
		return err
	}
	if err := validateTaskID(taskID); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.tombstones[taskID] = Tombstone{
		TenantID:  s.tenant.TenantID,
		Keyspace:  s.tenant.Keyspace,
		TaskID:    taskID,
		DeletedAt: deletedAt.UTC(),
	}
	return nil
}

func (s *MemoryStore) ready() error {
	if s.err != nil {
		return s.err
	}
	return nil
}

func (s *MemoryStore) validateManifest(manifest model.TaskManifest) error {
	return validateManifestForTenant(s.tenant, manifest)
}

type ExternalStore struct {
	tenant  model.TenantConfig
	layout  *Layout
	storage brstorage.ExternalStorage
}

func NewExternalStore(tenant model.TenantConfig, storage brstorage.ExternalStorage) (*ExternalStore, error) {
	layout, err := NewLayout(tenant)
	if err != nil {
		return nil, err
	}
	if storage == nil {
		return nil, fmt.Errorf("external storage must not be nil")
	}
	return &ExternalStore{tenant: tenant, layout: layout, storage: storage}, nil
}

func (s *ExternalStore) CreateManifest(ctx context.Context, manifest model.TaskManifest) error {
	if err := validateManifestForTenant(s.tenant, manifest); err != nil {
		return err
	}
	exists, err := s.storage.FileExists(ctx, s.layout.TaskManifestPath(manifest.TaskID))
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("task manifest %q already exists", manifest.TaskID)
	}
	tombstoned, err := s.storage.FileExists(ctx, s.layout.TombstonePath(manifest.TaskID))
	if err != nil {
		return err
	}
	if tombstoned {
		return fmt.Errorf("task manifest %q is tombstoned", manifest.TaskID)
	}
	return s.writeManifest(ctx, manifest)
}

func (s *ExternalStore) UpdateManifest(ctx context.Context, manifest model.TaskManifest) error {
	if err := validateManifestForTenant(s.tenant, manifest); err != nil {
		return err
	}
	tombstoned, err := s.storage.FileExists(ctx, s.layout.TombstonePath(manifest.TaskID))
	if err != nil {
		return err
	}
	if tombstoned {
		return fmt.Errorf("task manifest %q is tombstoned", manifest.TaskID)
	}
	return s.writeManifest(ctx, manifest)
}

func (s *ExternalStore) GetManifest(ctx context.Context, taskID string) (model.TaskManifest, error) {
	if err := validateTaskID(taskID); err != nil {
		return model.TaskManifest{}, err
	}
	tombstoned, err := s.storage.FileExists(ctx, s.layout.TombstonePath(taskID))
	if err != nil {
		return model.TaskManifest{}, err
	}
	if tombstoned {
		return model.TaskManifest{}, fmt.Errorf("task manifest %q is tombstoned", taskID)
	}
	data, err := s.storage.ReadFile(ctx, s.layout.TaskManifestPath(taskID))
	if err != nil {
		return model.TaskManifest{}, err
	}
	var manifest model.TaskManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return model.TaskManifest{}, err
	}
	if err := validateManifestForTenant(s.tenant, manifest); err != nil {
		return model.TaskManifest{}, err
	}
	return manifest, nil
}

func (s *ExternalStore) ListManifests(ctx context.Context) ([]model.TaskManifest, error) {
	manifests := make([]model.TaskManifest, 0)
	opt := &brstorage.WalkOption{SubDir: "metadata/tasks"}
	if err := s.storage.WalkDir(ctx, opt, func(path string, _ int64) error {
		if len(path) < len(".json") || path[len(path)-len(".json"):] != ".json" {
			return nil
		}
		data, err := s.storage.ReadFile(ctx, path)
		if err != nil {
			return err
		}
		var manifest model.TaskManifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			return err
		}
		if err := validateManifestForTenant(s.tenant, manifest); err != nil {
			return err
		}
		tombstoned, err := s.storage.FileExists(ctx, s.layout.TombstonePath(manifest.TaskID))
		if err != nil {
			return err
		}
		if !tombstoned {
			manifests = append(manifests, manifest)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Slice(manifests, func(i, j int) bool {
		return manifests[i].TaskID < manifests[j].TaskID
	})
	return manifests, nil
}

func (s *ExternalStore) TombstoneManifest(ctx context.Context, taskID string, deletedAt time.Time) error {
	if err := validateTaskID(taskID); err != nil {
		return err
	}
	tombstone := Tombstone{
		TenantID:  s.tenant.TenantID,
		Keyspace:  s.tenant.Keyspace,
		TaskID:    taskID,
		DeletedAt: deletedAt.UTC(),
	}
	data, err := json.MarshalIndent(tombstone, "", "  ")
	if err != nil {
		return err
	}
	return s.storage.WriteFile(ctx, s.layout.TombstonePath(taskID), data)
}

func (s *ExternalStore) writeManifest(ctx context.Context, manifest model.TaskManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	return s.storage.WriteFile(ctx, s.layout.TaskManifestPath(manifest.TaskID), data)
}

func validateManifestForTenant(tenant model.TenantConfig, manifest model.TaskManifest) error {
	if manifest.Version == 0 {
		return fmt.Errorf("manifest version must not be empty")
	}
	if manifest.TenantID != tenant.TenantID {
		return fmt.Errorf("manifest tenant_id %q does not match tenant context %q", manifest.TenantID, tenant.TenantID)
	}
	if manifest.Keyspace != tenant.Keyspace {
		return fmt.Errorf("manifest keyspace %q does not match tenant context %q", manifest.Keyspace, tenant.Keyspace)
	}
	if err := validateTaskID(manifest.TaskID); err != nil {
		return err
	}
	if manifest.Storage.URI != "" && manifest.Storage.URI != tenant.StorageURI {
		return fmt.Errorf("manifest storage uri must match tenant storage root")
	}
	return nil
}
