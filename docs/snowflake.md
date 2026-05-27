# Snowflake

## Replicate

To replicate snapshot and incremental data of a TiDB Table to Snowflake:

```shell
export AWS_ACCESS_KEY_ID=<ACCESS_KEY>
export AWS_SECRET_ACCESS_KEY=<SECRET_KEY>
export AWS_SESSION_TOKEN=<SESSION_TOKEN>  # Optional

./tidb2dw snowflake \
    --storage s3://my-demo-bucket/prefix \
    --table <database_name>.<table_name> \
    --snowflake.account-id <organization>-<account> \
    --snowflake.user <username> \
    --snowflake.pass <password> \
    --snowflake.database <database> \
    --snowflake.schema <schema> \

# Note that you may also need to specify these parameters:
#   --cdc.host x.x.x.x
#   --tidb.host x.x.x.x
#   --tidb.user <user>
#   --tidb.pass <pass>
# Use --help for details.
```

For sysbench multi-table tests, sysbench creates tables named
`<table-prefix>1` through `<table-prefix>N` in the database passed by
`--mysql-db`. For example, `--mysql-db=sbtest --tables=1000` creates
`sbtest1` through `sbtest1000`.

Use `--sysbench.tables` to let tidb2dw generate these table names. In this
mode, each table is replicated through its own S3 prefix and Snowflake external
stage:

```shell
./tidb2dw snowflake \
    --storage s3://my-demo-bucket/prefix \
    --sysbench.database sbtest \
    --sysbench.tables 1000 \
    --table-concurrency 64 \
    --snowflake.account-id <organization>-<account> \
    --snowflake.user <username> \
    --snowflake.pass <password> \
    --snowflake.database <database> \
    --snowflake.schema <schema>
```

`--table-concurrency` controls how many table-level workers run at the same
time. For sysbench runs with 1000 tables, increase it to raise TiDB, TiCDC, S3,
and Snowflake load. Keep `--snapshot-concurrency` in mind because it applies per
table.

## Supported DDL Operations

All DDL which will change the schema of table are supported (except index related), including:

- Add column
- Drop column
- Rename column
- Modify column type
- Drop table
- Truncate table

> **Note**
>
> 1. Snowflake does not support partition table, tidb2dw will view table with multiple partitions as ordinary table.
> 2. Snowflake has a lot of limitations on modifying column type, like Snowflake does not support update column default value, refer to [Snowflake Docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table-column).
> 3. The type mapping from TiDB to Snowflake is defined [here](https://github.com/pingcap-inc/tidb2dw/blob/main/pkg/snowsql/types.go).
