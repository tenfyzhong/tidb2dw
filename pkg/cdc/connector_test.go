package cdc

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCDCConnectorCreatesChangefeedWithStableIDAndColumnSelectors(t *testing.T) {
	var createReq map[string]interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/api/v2/changefeeds", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&createReq))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"tidb2dw-tenant-a-orders","config":{}}`))
	}))
	defer server.Close()

	storageURI, err := url.Parse("s3://bucket/tenants/tenant-a/data/tasks/orders/increment")
	require.NoError(t, err)

	connector, err := NewCDCConnectorWithOptions("127.0.0.1", 8300, []string{"orders.orders"}, 123, storageURI, time.Minute, 64*1024*1024, "hex", CDCConnectorOptions{
		Namespace:    "tenant-a",
		ChangefeedID: "tidb2dw-tenant-a-orders",
		ColumnSelectors: []ColumnSelector{
			{Matcher: []string{"orders.orders"}, Columns: []string{"id", "status"}},
		},
	})
	require.NoError(t, err)
	connector.cdcServer = server.URL

	require.NoError(t, connector.CreateChangefeed())
	require.Equal(t, "tenant-a", createReq["namespace"])
	require.Equal(t, "tidb2dw-tenant-a-orders", createReq["changefeed_id"])
	require.Equal(t, float64(123), createReq["start_ts"])

	replicaConfig := createReq["replica_config"].(map[string]interface{})
	sinkConfig := replicaConfig["sink"].(map[string]interface{})
	selectors := sinkConfig["column_selectors"].([]interface{})
	require.Len(t, selectors, 1)
	selector := selectors[0].(map[string]interface{})
	require.Equal(t, []interface{}{"orders.orders"}, selector["matcher"])
	require.Equal(t, []interface{}{"id", "status"}, selector["columns"])
}

func TestCDCConnectorLifecycleUsesStableChangefeedID(t *testing.T) {
	requests := make([]string, 0, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.String())
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	storageURI, err := url.Parse("s3://bucket/tenants/tenant-a/data/tasks/orders/increment")
	require.NoError(t, err)
	connector, err := NewCDCConnectorWithOptions("127.0.0.1", 8300, []string{"orders.orders"}, 0, storageURI, time.Minute, 64*1024*1024, "hex", CDCConnectorOptions{
		Namespace:    "tenant-a",
		ChangefeedID: "tidb2dw-tenant-a-orders",
	})
	require.NoError(t, err)
	connector.cdcServer = server.URL

	require.NoError(t, connector.PauseChangefeed())
	require.NoError(t, connector.ResumeChangefeed())
	require.NoError(t, connector.DeleteChangefeed())

	require.Equal(t, []string{
		"POST /api/v2/changefeeds/tidb2dw-tenant-a-orders/pause?namespace=tenant-a",
		"POST /api/v2/changefeeds/tidb2dw-tenant-a-orders/resume?namespace=tenant-a",
		"DELETE /api/v2/changefeeds/tidb2dw-tenant-a-orders?namespace=tenant-a",
	}, requests)
}
