package snowsql_test

import (
	"strings"
	"testing"

	"github.com/pingcap-inc/tidb2dw/pkg/model"
	"github.com/pingcap-inc/tidb2dw/pkg/snowsql"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/stretchr/testify/require"
)

func TestProjectSnowflakeColumnsKeepsRequiredKeysAndSnapshotPositions(t *testing.T) {
	columns := []cloudstorage.TableCol{
		{Name: "id", Tp: "int", IsPK: "true"},
		{Name: "customer_id", Tp: "int"},
		{Name: "status", Tp: "varchar"},
		{Name: "updated_at", Tp: "datetime"},
	}

	projected, positions, err := snowsql.ProjectSnowflakeColumns(columns, model.ColumnFilter{
		Mode:    model.ColumnFilterModeInclude,
		Columns: []string{"status", "updated_at"},
	}, []string{"id"})
	require.NoError(t, err)
	require.Equal(t, []string{"id", "status", "updated_at"}, snowflakeColumnNames(projected))
	require.Equal(t, []int{1, 3, 4}, positions)
}

func TestGenLoadSnapshotFromStageWithProjection(t *testing.T) {
	sql, err := snowsql.GenLoadSnapshotFromStageSQL(
		"orders",
		"snapshot_stage",
		"orders.orders.000001.csv",
		[]cloudstorage.TableCol{
			{Name: "id"},
			{Name: "status"},
		},
		[]int{1, 3},
	)
	require.NoError(t, err)
	require.Contains(t, sql, "COPY INTO orders (id, status)")
	require.Contains(t, sql, "SELECT $1, $3")
	require.Contains(t, sql, "FROM @snapshot_stage/orders.orders.000001.csv")
	require.False(t, strings.Contains(sql, "customer_id"))
}

func snowflakeColumnNames(columns []cloudstorage.TableCol) []string {
	names := make([]string, 0, len(columns))
	for _, column := range columns {
		names = append(names, column.Name)
	}
	return names
}
