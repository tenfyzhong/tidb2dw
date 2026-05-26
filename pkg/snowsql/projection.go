package snowsql

import (
	"fmt"
	"strings"

	"github.com/pingcap-inc/tidb2dw/pkg/filter"
	"github.com/pingcap-inc/tidb2dw/pkg/model"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
)

func ProjectSnowflakeColumns(
	columns []cloudstorage.TableCol,
	columnFilter model.ColumnFilter,
	requiredKeys []string,
) ([]cloudstorage.TableCol, []int, error) {
	projectedModelColumns, err := filter.ProjectColumns(toModelColumns(columns), columnFilter, requiredKeys, false)
	if err != nil {
		return nil, nil, err
	}

	columnByName := make(map[string]cloudstorage.TableCol, len(columns))
	positionByName := make(map[string]int, len(columns))
	for i, column := range columns {
		key := strings.ToLower(column.Name)
		columnByName[key] = column
		positionByName[key] = i + 1
	}

	projected := make([]cloudstorage.TableCol, 0, len(projectedModelColumns))
	positions := make([]int, 0, len(projectedModelColumns))
	for _, column := range projectedModelColumns {
		key := strings.ToLower(column.Name)
		projected = append(projected, columnByName[key])
		positions = append(positions, positionByName[key])
	}
	return projected, positions, nil
}

func ApplyTableBindingProjection(tableDef cloudstorage.TableDefinition, targetTable string, columnFilter model.ColumnFilter) (cloudstorage.TableDefinition, error) {
	return ApplyTableBindingProjectionWithPolicy(nil, tableDef, targetTable, columnFilter)
}

func ApplyTableBindingProjectionWithPolicy(
	prevProjectedColumns []cloudstorage.TableCol,
	tableDef cloudstorage.TableDefinition,
	targetTable string,
	columnFilter model.ColumnFilter,
) (cloudstorage.TableDefinition, error) {
	tableDef.Table = firstNonEmpty(targetTable, tableDef.Table)
	if columnFilter.Mode == "" {
		return tableDef, nil
	}
	switch columnFilter.OnSchemaChange {
	case "", model.ColumnFilterSchemaChangeIgnore, model.ColumnFilterSchemaChangeFail:
	default:
		return cloudstorage.TableDefinition{}, fmt.Errorf("unsupported column_filter.on_schema_change %q", columnFilter.OnSchemaChange)
	}
	originalColumnCount := len(tableDef.Columns)
	projected, _, err := ProjectSnowflakeColumns(tableDef.Columns, columnFilter, nil)
	if err != nil {
		return cloudstorage.TableDefinition{}, err
	}
	tableDef.Columns = projected
	if columnFilter.OnSchemaChange == model.ColumnFilterSchemaChangeFail &&
		len(prevProjectedColumns) > 0 &&
		originalColumnCount > len(projected) {
		diffs, err := tidbsql.GetColumnDiff(prevProjectedColumns, projected)
		if err != nil {
			return cloudstorage.TableDefinition{}, err
		}
		hasProjectedChange := false
		for _, diff := range diffs {
			if diff.Action != tidbsql.UNCHANGE {
				hasProjectedChange = true
				break
			}
		}
		if !hasProjectedChange {
			return cloudstorage.TableDefinition{}, fmt.Errorf("DDL affects a filtered-out column and column_filter.on_schema_change is fail")
		}
	}
	return tableDef, nil
}

func GenLoadSnapshotFromStageSQL(
	targetTable string,
	stageName string,
	filePath string,
	columns []cloudstorage.TableCol,
	positions []int,
) (string, error) {
	if len(columns) != len(positions) {
		return "", fmt.Errorf("columns and positions must have the same length")
	}
	if len(columns) == 0 {
		return "", fmt.Errorf("columns must not be empty")
	}

	columnNames := make([]string, 0, len(columns))
	selectExprs := make([]string, 0, len(positions))
	for i, column := range columns {
		columnNames = append(columnNames, column.Name)
		selectExprs = append(selectExprs, fmt.Sprintf("$%d", positions[i]))
	}
	return fmt.Sprintf(`COPY INTO %s (%s)
FROM (
    SELECT %s
    FROM @%s/%s
)
FILE_FORMAT = (TYPE = 'CSV' EMPTY_FIELD_AS_NULL = FALSE NULL_IF=('\\N') FIELD_OPTIONALLY_ENCLOSED_BY='"' ESCAPE='\\' BINARY_FORMAT = 'UTF8');`,
		targetTable,
		strings.Join(columnNames, ", "),
		strings.Join(selectExprs, ", "),
		stageName,
		filePath,
	), nil
}

func toModelColumns(columns []cloudstorage.TableCol) []model.Column {
	converted := make([]model.Column, 0, len(columns))
	for _, column := range columns {
		converted = append(converted, model.Column{
			ID:        column.ID,
			Name:      column.Name,
			Tp:        column.Tp,
			Default:   column.Default,
			Precision: column.Precision,
			Scale:     column.Scale,
			Nullable:  column.Nullable,
			IsPK:      column.IsPK,
		})
	}
	return converted
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
