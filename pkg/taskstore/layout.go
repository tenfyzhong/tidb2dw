package taskstore

import (
	"fmt"
	"net/url"
	"path"
	"strings"

	"github.com/pingcap-inc/tidb2dw/pkg/model"
)

type Layout struct {
	tenant model.TenantConfig
	root   *url.URL
}

func NewLayout(tenant model.TenantConfig) (*Layout, error) {
	if tenant.TenantID == "" {
		return nil, fmt.Errorf("tenant_id must not be empty")
	}
	if tenant.StorageURI == "" {
		return nil, fmt.Errorf("storage uri must not be empty")
	}
	root, err := url.Parse(tenant.StorageURI)
	if err != nil {
		return nil, fmt.Errorf("parse storage uri: %w", err)
	}
	if root.Scheme == "" || root.Host == "" {
		return nil, fmt.Errorf("storage uri must include scheme and bucket")
	}
	if !pathContainsSegment(root.Path, tenant.TenantID) {
		return nil, fmt.Errorf("storage uri path must include tenant_id %q", tenant.TenantID)
	}
	return &Layout{tenant: tenant, root: root}, nil
}

func (l *Layout) TaskManifestPath(taskID string) string {
	return path.Join("metadata", "tasks", safeTaskID(taskID)+".json")
}

func (l *Layout) TombstonePath(taskID string) string {
	return path.Join("metadata", "tombstones", safeTaskID(taskID)+".json")
}

func (l *Layout) RuntimeStatePath(taskID string) string {
	return path.Join("runtime", "tasks", safeTaskID(taskID), "state.json")
}

func (l *Layout) SnapshotLoadInfoPath(taskID string, table model.TableName) string {
	return path.Join("data", "tasks", safeTaskID(taskID), "snapshot", fmt.Sprintf("%s.%s.loadinfo", table.Database, table.Table))
}

func (l *Layout) StorageURI() string {
	root := *l.root
	return root.String()
}

func validateTaskID(taskID string) error {
	if taskID == "" {
		return fmt.Errorf("task_id must not be empty")
	}
	if taskID == "." || taskID == ".." || strings.Contains(taskID, "/") || strings.Contains(taskID, "\\") {
		return fmt.Errorf("task_id %q must be a single path segment", taskID)
	}
	return nil
}

func safeTaskID(taskID string) string {
	if err := validateTaskID(taskID); err != nil {
		return "_invalid_task_id_"
	}
	return taskID
}

func pathContainsSegment(pathValue string, segment string) bool {
	parts := strings.Split(strings.Trim(pathValue, "/"), "/")
	for _, part := range parts {
		if part == segment {
			return true
		}
	}
	return false
}
