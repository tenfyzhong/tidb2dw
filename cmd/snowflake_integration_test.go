//go:build integration

package cmd

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/cdc"
	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/snowsql"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap-inc/tidb2dw/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestSnowflakeSnapshotIntegration(t *testing.T) {
	if os.Getenv("TIDB2DW_RUN_INTEGRATION") != "1" {
		t.Skip("set TIDB2DW_RUN_INTEGRATION=1 to run the external Snowflake integration test")
	}

	cfg := loadIntegrationConfig(t)
	storageURI, err := getS3URIWithCredentials(cfg.storageURI, &cfg.awsCredentials)
	require.NoError(t, err)
	snapshotURI, incrementURI, err := genSnapshotAndIncrementURIs(storageURI)
	require.NoError(t, err)

	snapConnectorMap := make(map[string]coreinterfaces.Connector)
	increConnectorMap := make(map[string]coreinterfaces.Connector)
	for _, tableFQN := range cfg.tables {
		sourceDatabase, sourceTable := utils.SplitTableFQN(tableFQN)
		snapConnector, err := snowsql.NewSnowflakeConnector(
			&cfg.snowflake,
			"snapshot_integration_"+sourceDatabase+"_"+sourceTable,
			snapshotURI,
			&cfg.awsCredentials,
		)
		require.NoError(t, err)
		snapConnectorMap[tableFQN] = snapConnector

		increConnector, err := snowsql.NewSnowflakeConnector(
			&cfg.snowflake,
			"increment_integration_"+sourceDatabase+"_"+sourceTable,
			incrementURI,
			&cfg.awsCredentials,
		)
		require.NoError(t, err)
		increConnectorMap[tableFQN] = increConnector
	}
	defer closeConnectors(snapConnectorMap)
	defer closeConnectors(increConnectorMap)

	require.NoError(t, ReplicateWithCDCConfig(
		context.Background(),
		&cfg.tidb,
		cfg.tables,
		storageURI,
		snapshotURI,
		incrementURI,
		cfg.snapshotConcurrency,
		cfg.cdcHost,
		cfg.cdcPort,
		time.Minute,
		64*1024*1024,
		snapConnectorMap,
		increConnectorMap,
		"snowflake",
		true,
		RunModeSnapshotOnly,
		CDCExportConfig{},
	))
}

func TestTiCDCChangefeedLifecycleIntegration(t *testing.T) {
	if os.Getenv("TIDB2DW_RUN_INTEGRATION") != "1" {
		t.Skip("set TIDB2DW_RUN_INTEGRATION=1 to run the external TiCDC integration test")
	}

	cfg := loadIntegrationConfig(t)
	incrementURI, err := getS3URIWithCredentials(cfg.storageURI, &cfg.awsCredentials)
	require.NoError(t, err)
	incrementURI.Path = strings.TrimRight(incrementURI.Path, "/") + "/integration/ticdc"

	startTSO, err := tidbsql.GetCurrentTSO(&cfg.tidb)
	require.NoError(t, err)

	connector, err := cdc.NewCDCConnectorWithOptions(
		cfg.cdcHost,
		cfg.cdcPort,
		cfg.tables,
		startTSO,
		incrementURI,
		time.Minute,
		64*1024*1024,
		CdcCsvBinaryEncodingMethodMap["snowflake"],
		cdc.CDCConnectorOptions{
			Namespace:    "integration",
			ChangefeedID: "tidb2dw-integration-lifecycle",
		},
	)
	require.NoError(t, err)

	require.NoError(t, connector.CreateChangefeed())
	require.NoError(t, connector.PauseChangefeed())
	require.NoError(t, connector.ResumeChangefeed())
	require.NoError(t, connector.DeleteChangefeed())
}

type integrationConfig struct {
	tidb                tidbsql.TiDBConfig
	snowflake           snowsql.SnowflakeConfig
	awsCredentials      credentials.Value
	tables              []string
	storageURI          string
	cdcHost             string
	cdcPort             int
	snapshotConcurrency int
}

func loadIntegrationConfig(t *testing.T) integrationConfig {
	t.Helper()

	cdcPort, err := strconv.Atoi(requiredEnv(t, "TIDB2DW_CDC_PORT"))
	require.NoError(t, err)
	tidbPort, err := strconv.Atoi(requiredEnv(t, "TIDB2DW_TIDB_PORT"))
	require.NoError(t, err)

	tables := strings.Split(requiredEnv(t, "TIDB2DW_TABLES"), ",")
	for i := range tables {
		tables[i] = strings.TrimSpace(tables[i])
	}

	return integrationConfig{
		tidb: tidbsql.TiDBConfig{
			Host: requiredEnv(t, "TIDB2DW_TIDB_HOST"),
			Port: tidbPort,
			User: requiredEnv(t, "TIDB2DW_TIDB_USER"),
			Pass: os.Getenv("TIDB2DW_TIDB_PASS"),
		},
		snowflake: snowsql.SnowflakeConfig{
			AccountId: requiredEnv(t, "TIDB2DW_SNOWFLAKE_ACCOUNT_ID"),
			Warehouse: requiredEnv(t, "TIDB2DW_SNOWFLAKE_WAREHOUSE"),
			User:      requiredEnv(t, "TIDB2DW_SNOWFLAKE_USER"),
			Pass:      requiredEnv(t, "TIDB2DW_SNOWFLAKE_PASS"),
			Database:  requiredEnv(t, "TIDB2DW_SNOWFLAKE_DATABASE"),
			Schema:    requiredEnv(t, "TIDB2DW_SNOWFLAKE_SCHEMA"),
		},
		awsCredentials: credentials.Value{
			AccessKeyID:     requiredEnv(t, "AWS_ACCESS_KEY_ID"),
			SecretAccessKey: requiredEnv(t, "AWS_SECRET_ACCESS_KEY"),
			SessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
		},
		tables:              tables,
		storageURI:          requiredEnv(t, "TIDB2DW_STORAGE_URI"),
		cdcHost:             requiredEnv(t, "TIDB2DW_CDC_HOST"),
		cdcPort:             cdcPort,
		snapshotConcurrency: 4,
	}
}

func requiredEnv(t *testing.T, key string) string {
	t.Helper()
	value := os.Getenv(key)
	require.NotEmpty(t, value, "%s must be set", key)
	return value
}
