package snowsql

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/model"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"go.uber.org/zap"
)

// A Wrapper of snowflake connection.
// It implements the coreinterfaces.Connector interface.
type SnowflakeConnector struct {
	// db is the connection to snowflake.
	db *sql.DB

	stageName string

	s3Credentials *credentials.Value

	columns []cloudstorage.TableCol

	targetTable             string
	columnFilter            model.ColumnFilter
	snapshotColumns         []cloudstorage.TableCol
	snapshotColumnPositions []int
}

func NewSnowflakeConnector(sfConfig *SnowflakeConfig, stageName string, storageURI *url.URL, credentials *credentials.Value) (*SnowflakeConnector, error) {
	return NewSnowflakeConnectorWithOptions(sfConfig, stageName, storageURI, credentials, SnowflakeConnectorOptions{})
}

type SnowflakeConnectorOptions struct {
	TargetTable  string
	ColumnFilter model.ColumnFilter
}

func NewSnowflakeConnectorWithOptions(
	sfConfig *SnowflakeConfig,
	stageName string,
	storageURI *url.URL,
	credentials *credentials.Value,
	options SnowflakeConnectorOptions,
) (*SnowflakeConnector, error) {
	db, err := sfConfig.OpenDB()
	if err != nil {
		return nil, errors.Trace(err)
	}
	// create stage
	stageUrl := fmt.Sprintf("%s://%s%s", storageURI.Scheme, storageURI.Host, storageURI.Path)
	if err := CreateExternalStage(db, stageName, stageUrl, credentials); err != nil {
		return nil, errors.Annotate(err, "Failed to create stage")
	}

	return &SnowflakeConnector{
		db:            db,
		stageName:     stageName,
		s3Credentials: credentials,
		columns:       nil,
		targetTable:   options.TargetTable,
		columnFilter:  options.ColumnFilter,
	}, nil
}

func (sc *SnowflakeConnector) InitSchema(columns []cloudstorage.TableCol) error {
	if len(sc.columns) != 0 {
		return nil
	}
	var err error
	columns, err = sc.projectColumns(columns, nil)
	if err != nil {
		return errors.Trace(err)
	}
	if len(columns) == 0 {
		return errors.New("Columns in schema is empty")
	}
	sc.columns = columns
	log.Info("table columns initialized", zap.Any("Columns", columns))
	return nil
}

func (sc *SnowflakeConnector) ExecDDL(tableDef cloudstorage.TableDefinition) error {
	if len(sc.columns) == 0 {
		return errors.New("Columns not initialized. Maybe you execute a DDL before all DMLs, which is not supported now.")
	}
	var err error
	tableDef, err = ApplyTableBindingProjection(tableDef, sc.targetTable, sc.columnFilter)
	if err != nil {
		return errors.Trace(err)
	}
	ddls, err := GenDDLViaColumnsDiff(sc.columns, tableDef)
	if err != nil {
		return errors.Trace(err)
	}
	if len(ddls) == 0 {
		log.Info("No need to execute this DDL in Snowflake", zap.String("ddl", tableDef.Query))
		return nil
	}
	// One DDL may be rewritten to multiple DDLs
	for _, ddl := range ddls {
		_, err := sc.db.Exec(ddl)
		if err != nil {
			log.Error("Failed to executed DDL", zap.String("received", tableDef.Query), zap.String("rewritten", strings.Join(ddls, "\n")))
			return errors.Annotate(err, fmt.Sprint("failed to execute", ddl))
		}
	}
	// update columns
	sc.columns = tableDef.Columns
	log.Info("Successfully executed DDL", zap.String("received", tableDef.Query), zap.String("rewritten", strings.Join(ddls, "\n")))
	return nil
}

func (sc *SnowflakeConnector) CopyTableSchema(sourceDatabase string, sourceTable string, sourceTiDBConn *sql.DB) error {
	tableColumns, err := tidbsql.GetTiDBTableColumn(sourceTiDBConn, sourceDatabase, sourceTable)
	if err != nil {
		return errors.Trace(err)
	}
	snowflakePKColumns, err := tidbsql.GetTiDBTablePKColumns(sourceTiDBConn, sourceDatabase, sourceTable)
	if err != nil {
		return errors.Trace(err)
	}
	tableColumns, sc.snapshotColumnPositions, err = sc.projectColumnsWithPositions(tableColumns, snowflakePKColumns)
	if err != nil {
		return errors.Trace(err)
	}
	sc.snapshotColumns = tableColumns
	sc.columns = tableColumns

	targetTable := sc.resolveTargetTable(sourceTable)
	createTableQuery, err := GenCreateSchemaWithColumns(targetTable, tableColumns, snowflakePKColumns)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Creating table in Snowflake", zap.String("query", createTableQuery))
	_, err = sc.db.Exec(createTableQuery)
	return err
}

func (sc *SnowflakeConnector) LoadSnapshot(targetTable, filePath string) error {
	targetTable = sc.resolveTargetTable(targetTable)
	var err error
	if len(sc.snapshotColumnPositions) > 0 {
		err = LoadSnapshotFromStageWithProjection(sc.db, targetTable, sc.stageName, filePath, sc.snapshotColumns, sc.snapshotColumnPositions)
	} else {
		err = LoadSnapshotFromStage(sc.db, targetTable, sc.stageName, filePath)
	}
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (sc *SnowflakeConnector) LoadIncrement(tableDef cloudstorage.TableDefinition, filePath string) error {
	var err error
	tableDef, err = ApplyTableBindingProjection(tableDef, sc.targetTable, sc.columnFilter)
	if err != nil {
		return errors.Trace(err)
	}
	// merge staged file into table
	mergeQuery := GenMergeInto(tableDef, filePath, sc.stageName)
	_, err = sc.db.Exec(mergeQuery)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Successfully merge file", zap.String("file", filePath))
	return nil
}

func (sc *SnowflakeConnector) Close() {
	// drop stage
	if err := DropStage(sc.db, sc.stageName); err != nil {
		log.Error("fail to drop stage", zap.Error(err))
	}
	sc.db.Close()
}

func (sc *SnowflakeConnector) projectColumns(columns []cloudstorage.TableCol, requiredKeys []string) ([]cloudstorage.TableCol, error) {
	projected, _, err := sc.projectColumnsWithPositions(columns, requiredKeys)
	return projected, err
}

func (sc *SnowflakeConnector) projectColumnsWithPositions(columns []cloudstorage.TableCol, requiredKeys []string) ([]cloudstorage.TableCol, []int, error) {
	if sc.columnFilter.Mode == "" {
		return columns, nil, nil
	}
	return ProjectSnowflakeColumns(columns, sc.columnFilter, requiredKeys)
}

func (sc *SnowflakeConnector) resolveTargetTable(defaultTable string) string {
	if sc.targetTable != "" {
		return sc.targetTable
	}
	return defaultTable
}
