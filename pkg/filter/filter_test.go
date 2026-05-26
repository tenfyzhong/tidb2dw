package filter_test

import (
	"testing"

	"github.com/pingcap-inc/tidb2dw/pkg/filter"
	"github.com/pingcap-inc/tidb2dw/pkg/model"
	"github.com/stretchr/testify/require"
)

func TestResolveTablesAppliesIncludeExcludeAndSystemSchemaGuard(t *testing.T) {
	candidates := []model.TableName{
		{Database: "orders", Table: "orders"},
		{Database: "orders", Table: "tmp_202605"},
		{Database: "orders", Table: "shadow_events"},
		{Database: "billing", Table: "invoices"},
		{Database: "mysql", Table: "user"},
	}

	got, err := filter.ResolveTables(model.TableFilter{
		Include:       []string{"orders.*", "billing.invoices", "!orders.tmp_*", "!*.shadow_*"},
		CaseSensitive: false,
	}, candidates)
	require.NoError(t, err)
	require.Equal(t, []model.TableName{
		{Database: "orders", Table: "orders"},
		{Database: "billing", Table: "invoices"},
	}, got)
}

func TestResolveTablesRejectsEmptyInclude(t *testing.T) {
	_, err := filter.ResolveTables(model.TableFilter{}, []model.TableName{
		{Database: "orders", Table: "orders"},
	})
	require.ErrorContains(t, err, "include")
}

func TestResolveTablesRejectsMatchedSystemSchema(t *testing.T) {
	_, err := filter.ResolveTables(model.TableFilter{
		Include: []string{"*.*"},
	}, []model.TableName{
		{Database: "mysql", Table: "user"},
	})
	require.ErrorContains(t, err, "system schema")
}

func TestProjectColumnsIncludeAddsRequiredKeys(t *testing.T) {
	columns := []model.Column{
		{Name: "id", Tp: "int", IsPK: "true"},
		{Name: "customer_id", Tp: "int"},
		{Name: "status", Tp: "varchar"},
		{Name: "updated_at", Tp: "datetime"},
	}

	projected, err := filter.ProjectColumns(columns, model.ColumnFilter{
		Mode:    model.ColumnFilterModeInclude,
		Columns: []string{"status", "updated_at"},
	}, []string{"id"}, false)
	require.NoError(t, err)
	require.Equal(t, []string{"id", "status", "updated_at"}, columnNames(projected))
}

func TestProjectColumnsExcludeKeepsRequiredKeys(t *testing.T) {
	columns := []model.Column{
		{Name: "id", Tp: "int", IsPK: "true"},
		{Name: "raw_payload", Tp: "json"},
		{Name: "updated_at", Tp: "datetime"},
	}

	projected, err := filter.ProjectColumns(columns, model.ColumnFilter{
		Mode:    model.ColumnFilterModeExclude,
		Exclude: []string{"id", "raw_payload"},
	}, []string{"id"}, false)
	require.NoError(t, err)
	require.Equal(t, []string{"id", "updated_at"}, columnNames(projected))
}

func TestProjectColumnsRejectsUnknownColumn(t *testing.T) {
	_, err := filter.ProjectColumns([]model.Column{
		{Name: "id", Tp: "int", IsPK: "true"},
	}, model.ColumnFilter{
		Mode:    model.ColumnFilterModeInclude,
		Columns: []string{"missing"},
	}, []string{"id"}, false)
	require.ErrorContains(t, err, "unknown column")
}

func columnNames(columns []model.Column) []string {
	names := make([]string, 0, len(columns))
	for _, column := range columns {
		names = append(names, column.Name)
	}
	return names
}
