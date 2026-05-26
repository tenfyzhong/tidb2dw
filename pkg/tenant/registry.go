package tenant

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/pingcap-inc/tidb2dw/pkg/model"
)

func LoadRegistryFile(path string) (model.TenantRegistry, error) {
	if path == "" {
		return model.TenantRegistry{}, fmt.Errorf("tenant registry path must not be empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return model.TenantRegistry{}, err
	}
	var registry model.TenantRegistry
	if err := json.Unmarshal(data, &registry); err != nil {
		return model.TenantRegistry{}, err
	}
	if len(registry.Tenants) == 0 {
		return model.TenantRegistry{}, fmt.Errorf("tenant registry must contain at least one tenant")
	}
	if err := resolveRegistrySecrets(&registry); err != nil {
		return model.TenantRegistry{}, err
	}
	return registry, nil
}

func resolveRegistrySecrets(registry *model.TenantRegistry) error {
	for i := range registry.Tenants {
		tenant := &registry.Tenants[i]
		var err error
		if tenant.Auth.BearerToken, err = resolveSecretRef(tenant.Auth.BearerToken); err != nil {
			return fmt.Errorf("resolve tenant %q auth bearer_token: %w", tenant.TenantID, err)
		}
		if tenant.StorageCredentials.SecretAccessKey, err = resolveSecretRef(tenant.StorageCredentials.SecretAccessKey); err != nil {
			return fmt.Errorf("resolve tenant %q storage secret_access_key: %w", tenant.TenantID, err)
		}
		if tenant.StorageCredentials.SessionToken, err = resolveSecretRef(tenant.StorageCredentials.SessionToken); err != nil {
			return fmt.Errorf("resolve tenant %q storage session_token: %w", tenant.TenantID, err)
		}
		if tenant.Source.TiDB.Pass, err = resolveSecretRef(tenant.Source.TiDB.Pass); err != nil {
			return fmt.Errorf("resolve tenant %q tidb pass: %w", tenant.TenantID, err)
		}
		if tenant.Sink.Pass, err = resolveSecretRef(tenant.Sink.Pass); err != nil {
			return fmt.Errorf("resolve tenant %q sink pass: %w", tenant.TenantID, err)
		}
	}
	return nil
}

func resolveSecretRef(value string) (string, error) {
	switch {
	case strings.HasPrefix(value, "env:"):
		key := strings.TrimPrefix(value, "env:")
		if key == "" {
			return "", fmt.Errorf("env secret ref must include a variable name")
		}
		resolved, ok := os.LookupEnv(key)
		if !ok {
			return "", fmt.Errorf("environment variable %q is not set", key)
		}
		return resolved, nil
	case strings.HasPrefix(value, "file:"):
		path := strings.TrimPrefix(value, "file:")
		if path == "" {
			return "", fmt.Errorf("file secret ref must include a path")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(data)), nil
	default:
		return value, nil
	}
}
