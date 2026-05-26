package model

import (
	"fmt"
)

type TaskMode string

const (
	TaskModeFull            TaskMode = "full"
	TaskModeSnapshotOnly    TaskMode = "snapshot-only"
	TaskModeIncrementalOnly TaskMode = "incremental-only"
)

type TaskStatus string

const (
	TaskStatusCreating TaskStatus = "creating"
	TaskStatusRunning  TaskStatus = "running"
	TaskStatusPaused   TaskStatus = "paused"
	TaskStatusDeleting TaskStatus = "deleting"
	TaskStatusDeleted  TaskStatus = "deleted"
	TaskStatusFailed   TaskStatus = "failed"
)

type SinkType string

const (
	SinkTypeSnowflake SinkType = "snowflake"
)

type ColumnFilterMode string

const (
	ColumnFilterModeInclude ColumnFilterMode = "include"
	ColumnFilterModeExclude ColumnFilterMode = "exclude"
)

const (
	ColumnFilterSchemaChangeIgnore = "ignore"
	ColumnFilterSchemaChangeFail   = "fail"
)

type TenantRegistry struct {
	Tenants []TenantConfig `json:"tenants"`
}

type TenantConfig struct {
	TenantID           string             `json:"tenant_id"`
	Keyspace           string             `json:"keyspace"`
	StorageURI         string             `json:"storage_uri"`
	Auth               TenantAuth         `json:"auth,omitempty"`
	StorageCredentials AWSCredentials     `json:"storage_credentials,omitempty"`
	Source             TenantSourceConfig `json:"source,omitempty"`
	Sink               SinkConfig         `json:"sink,omitempty"`
	SinkPolicy         SinkPolicy         `json:"sink_policy,omitempty"`
	Limits             TenantLimits       `json:"limits,omitempty"`
}

type TenantAuth struct {
	BearerToken string `json:"bearer_token,omitempty"`
}

type TenantSourceConfig struct {
	TiDB SourceTiDBConfig `json:"tidb,omitempty"`
	CDC  SourceCDCConfig  `json:"cdc,omitempty"`
}

type AWSCredentials struct {
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
	SessionToken    string `json:"session_token,omitempty"`
}

type SinkPolicy struct {
	AllowedDatabases []string `json:"allowed_databases,omitempty"`
	AllowedSchemas   []string `json:"allowed_schemas,omitempty"`
}

type TenantLimits struct {
	SnapshotConcurrency  int    `json:"snapshot_concurrency,omitempty"`
	MaxTableWorkers      int    `json:"max_table_workers,omitempty"`
	PendingIncrementByte int64  `json:"pending_increment_bytes,omitempty"`
	CDCFlushInterval     string `json:"cdc_flush_interval,omitempty"`
	CDCFileSize          int    `json:"cdc_file_size,omitempty"`
}

type TaskManifest struct {
	Version   int           `json:"version"`
	TenantID  string        `json:"tenant_id"`
	Keyspace  string        `json:"keyspace"`
	TaskID    string        `json:"task_id"`
	Mode      TaskMode      `json:"mode"`
	Status    TaskStatus    `json:"status"`
	Source    TaskSource    `json:"source"`
	Storage   StorageConfig `json:"storage"`
	Sink      SinkConfig    `json:"sink"`
	Limits    TaskLimits    `json:"limits,omitempty"`
	CreatedAt Time          `json:"created_at,omitempty"`
	UpdatedAt Time          `json:"updated_at,omitempty"`
}

type CreateTaskRequest struct {
	TenantID string        `json:"tenant_id,omitempty"`
	Keyspace string        `json:"keyspace,omitempty"`
	TaskID   string        `json:"task_id"`
	Mode     TaskMode      `json:"mode"`
	Source   TaskSource    `json:"source"`
	Storage  StorageConfig `json:"storage"`
	Sink     SinkConfig    `json:"sink"`
	Limits   TaskLimits    `json:"limits,omitempty"`
}

type TaskSource struct {
	TiDB        SourceTiDBConfig `json:"tidb,omitempty"`
	CDC         SourceCDCConfig  `json:"cdc,omitempty"`
	TableFilter TableFilter      `json:"table_filter"`
	Tables      []TableBinding   `json:"tables,omitempty"`
}

type SourceTiDBConfig struct {
	Host  string `json:"host,omitempty"`
	Port  int    `json:"port,omitempty"`
	User  string `json:"user,omitempty"`
	Pass  string `json:"pass,omitempty"`
	SSLCA string `json:"ssl_ca,omitempty"`
}

type SourceCDCConfig struct {
	Host         string `json:"host,omitempty"`
	Port         int    `json:"port,omitempty"`
	Namespace    string `json:"namespace,omitempty"`
	ChangefeedID string `json:"changefeed_id,omitempty"`
}

type TableFilter struct {
	Include       []string `json:"include"`
	Exclude       []string `json:"exclude,omitempty"`
	CaseSensitive bool     `json:"case_sensitive,omitempty"`
}

type ColumnFilter struct {
	Mode           ColumnFilterMode `json:"mode,omitempty"`
	Columns        []string         `json:"columns,omitempty"`
	Exclude        []string         `json:"exclude,omitempty"`
	OnSchemaChange string           `json:"on_schema_change,omitempty"`
}

type TableBinding struct {
	Database       string       `json:"database"`
	Table          string       `json:"table"`
	TargetDatabase string       `json:"target_database,omitempty"`
	TargetSchema   string       `json:"target_schema,omitempty"`
	TargetTable    string       `json:"target_table,omitempty"`
	ColumnFilter   ColumnFilter `json:"column_filter,omitempty"`
}

type StorageConfig struct {
	URI string `json:"uri"`
}

type SinkConfig struct {
	Type        SinkType `json:"type"`
	AccountID   string   `json:"account_id,omitempty"`
	Warehouse   string   `json:"warehouse,omitempty"`
	User        string   `json:"user,omitempty"`
	Pass        string   `json:"pass,omitempty"`
	Database    string   `json:"database,omitempty"`
	Schema      string   `json:"schema,omitempty"`
	TableNaming string   `json:"table_naming,omitempty"`
}

type TaskLimits struct {
	SnapshotConcurrency int    `json:"snapshot_concurrency,omitempty"`
	MaxTableWorkers     int    `json:"max_table_workers,omitempty"`
	CDCFlushInterval    string `json:"cdc_flush_interval,omitempty"`
	CDCFileSize         int    `json:"cdc_file_size,omitempty"`
}

type TableName struct {
	Database string `json:"database"`
	Table    string `json:"table"`
}

func (t TableName) String() string {
	return fmt.Sprintf("%s.%s", t.Database, t.Table)
}

type Column struct {
	ID        string      `json:"id,omitempty"`
	Name      string      `json:"name"`
	Tp        string      `json:"type,omitempty"`
	Default   interface{} `json:"default,omitempty"`
	Precision string      `json:"precision,omitempty"`
	Scale     string      `json:"scale,omitempty"`
	Nullable  string      `json:"nullable,omitempty"`
	IsPK      string      `json:"is_pk,omitempty"`
}
