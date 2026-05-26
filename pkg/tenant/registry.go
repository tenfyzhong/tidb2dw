package tenant

import (
	"encoding/json"
	"fmt"
	"os"

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
	return registry, nil
}
