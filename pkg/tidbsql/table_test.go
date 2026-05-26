package tidbsql_test

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap-inc/tidb2dw/pkg/model"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/stretchr/testify/require"
)

func TestListTiDBTables(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("SELECT TABLE_SCHEMA, TABLE_NAME").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME"}).
			AddRow("orders", "orders").
			AddRow("billing", "invoices"))

	tables, err := tidbsql.ListTiDBTables(db)
	require.NoError(t, err)
	require.Equal(t, []model.TableName{
		{Database: "orders", Table: "orders"},
		{Database: "billing", Table: "invoices"},
	}, tables)
	require.NoError(t, mock.ExpectationsWereMet())
}
