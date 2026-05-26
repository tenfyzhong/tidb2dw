package apiservice_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pingcap-inc/tidb2dw/pkg/apiservice"
	"github.com/pingcap-inc/tidb2dw/pkg/model"
	"github.com/pingcap-inc/tidb2dw/pkg/taskstore"
	"github.com/pingcap-inc/tidb2dw/pkg/tenant"
	"github.com/stretchr/testify/require"
)

func TestTenantAPICreatesAndListsTenantScopedTasks(t *testing.T) {
	manager := newAPITestManager(t)
	service := apiservice.New()
	service.RegisterTenantManager(manager)

	body, err := json.Marshal(model.CreateTaskRequest{
		TaskID: "orders-to-snowflake",
		Mode:   model.TaskModeFull,
		Source: model.TaskSource{
			TableFilter: model.TableFilter{Include: []string{"orders.*"}},
			Tables: []model.TableBinding{
				{Database: "orders", Table: "orders", TargetDatabase: "ANALYTICS", TargetSchema: "TENANT_A", TargetTable: "orders"},
			},
		},
		Storage: model.StorageConfig{URI: "s3://company-replication/tenants/tenant-a"},
		Sink:    model.SinkConfig{Type: model.SinkTypeSnowflake, Database: "ANALYTICS", Schema: "TENANT_A"},
	})
	require.NoError(t, err)

	createRecorder := httptest.NewRecorder()
	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/tenants/tenant-a/tasks", bytes.NewReader(body))
	createReq.Header.Set("Content-Type", "application/json")
	service.ServeHTTP(createRecorder, createReq)
	require.Equal(t, http.StatusCreated, createRecorder.Code)

	var created model.TaskManifest
	require.NoError(t, json.Unmarshal(createRecorder.Body.Bytes(), &created))
	require.Equal(t, "tenant-a", created.TenantID)
	require.Equal(t, "keyspace_a", created.Keyspace)

	listRecorder := httptest.NewRecorder()
	service.ServeHTTP(listRecorder, httptest.NewRequest(http.MethodGet, "/api/v1/tenants/tenant-a/tasks", nil))
	require.Equal(t, http.StatusOK, listRecorder.Code)

	var listed []model.TaskManifest
	require.NoError(t, json.Unmarshal(listRecorder.Body.Bytes(), &listed))
	require.Len(t, listed, 1)
	require.Equal(t, "orders-to-snowflake", listed[0].TaskID)
}

func TestTenantAPIRejectsCrossTenantBody(t *testing.T) {
	manager := newAPITestManager(t)
	service := apiservice.New()
	service.RegisterTenantManager(manager)

	body, err := json.Marshal(model.CreateTaskRequest{
		TenantID: "tenant-b",
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
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tenants/tenant-a/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	service.ServeHTTP(recorder, req)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "tenant_id")
}

func TestTenantAPIInfoIncludesLoadedTenantCount(t *testing.T) {
	manager := newAPITestManager(t)
	service := apiservice.New()
	service.RegisterTenantManager(manager)

	recorder := httptest.NewRecorder()
	service.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/info", nil))
	require.Equal(t, http.StatusOK, recorder.Code)

	var info apiservice.ProcessInfoResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &info))
	require.Equal(t, 1, info.LoadedTenantCount)
	require.Equal(t, apiservice.ServiceStatusRunning, info.Status)
}

func newAPITestManager(t *testing.T) *tenant.Manager {
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
			},
		},
	}, taskstore.NewMemoryStoreFactory())
	require.NoError(t, err)
	return manager
}
