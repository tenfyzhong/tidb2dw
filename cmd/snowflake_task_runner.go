package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap-inc/tidb2dw/pkg/coreinterfaces"
	"github.com/pingcap-inc/tidb2dw/pkg/model"
	"github.com/pingcap-inc/tidb2dw/pkg/snowsql"
	"github.com/pingcap-inc/tidb2dw/pkg/taskstore"
	"github.com/pingcap-inc/tidb2dw/pkg/tidbsql"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type SnowflakeTaskRunnerConfig struct {
	DefaultTiDB                tidbsql.TiDBConfig
	DefaultCDC                 model.SourceCDCConfig
	DefaultSnowflake           snowsql.SnowflakeConfig
	DefaultAWSCredentials      credentials.Value
	DefaultSnapshotConcurrency int
	DefaultCDCFlushInterval    time.Duration
	DefaultCDCFileSize         int
}

type SnowflakeTaskRunner struct {
	cfg SnowflakeTaskRunnerConfig

	mu      sync.Mutex
	cancels map[string]context.CancelFunc

	validateTask func(ctx context.Context, cfg snowflakeTaskConfig) error
	runTask      func(ctx context.Context, cfg snowflakeTaskConfig) error
}

type snowflakeTaskConfig struct {
	tenantConfig        model.TenantConfig
	manifest            model.TaskManifest
	tidbConfig          tidbsql.TiDBConfig
	cdcConfig           model.SourceCDCConfig
	snowflakeConfig     snowsql.SnowflakeConfig
	awsCredentials      credentials.Value
	storageURI          *url.URL
	bindings            []model.TableBinding
	tables              []string
	snapshotConcurrency int
	cdcFlushInterval    time.Duration
	cdcFileSize         int
	mode                RunMode
}

func NewSnowflakeTaskRunner(cfg SnowflakeTaskRunnerConfig) *SnowflakeTaskRunner {
	runner := &SnowflakeTaskRunner{
		cfg:     cfg,
		cancels: make(map[string]context.CancelFunc),
	}
	runner.validateTask = runner.validateSnowflakeTask
	runner.runTask = runner.runSnowflakeTask
	return runner
}

func (r *SnowflakeTaskRunner) ValidateTask(ctx context.Context, tenantConfig model.TenantConfig, manifest model.TaskManifest) error {
	cfg, err := r.buildConfig(tenantConfig, manifest)
	if err != nil {
		return err
	}
	return r.validateTask(ctx, cfg)
}

func (r *SnowflakeTaskRunner) StartTask(ctx context.Context, tenantConfig model.TenantConfig, manifest model.TaskManifest, store taskstore.Store) error {
	cfg, err := r.buildConfig(tenantConfig, manifest)
	if err != nil {
		return err
	}

	taskKey := runnerTaskKey(tenantConfig.TenantID, manifest.TaskID)
	r.mu.Lock()
	if _, ok := r.cancels[taskKey]; ok {
		r.mu.Unlock()
		return nil
	}
	taskCtx, cancel := context.WithCancel(ctx)
	r.cancels[taskKey] = cancel
	r.mu.Unlock()

	manifest.Status = model.TaskStatusRunning
	manifest.UpdatedAt = time.Now().UTC()
	if err := store.UpdateManifest(ctx, manifest); err != nil {
		r.forgetTask(taskKey)
		cancel()
		return err
	}

	go func() {
		defer r.forgetTask(taskKey)
		if err := r.runTask(taskCtx, cfg); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Error("Tenant task failed", zap.String("tenant", tenantConfig.TenantID), zap.String("task", manifest.TaskID), zap.Error(err))
			failed := manifest
			failed.Status = model.TaskStatusFailed
			failed.UpdatedAt = time.Now().UTC()
			if updateErr := store.UpdateManifest(context.Background(), failed); updateErr != nil {
				log.Error("Failed to update failed task manifest", zap.String("tenant", tenantConfig.TenantID), zap.String("task", manifest.TaskID), zap.Error(updateErr))
			}
		}
	}()

	return nil
}

func (r *SnowflakeTaskRunner) StopTask(_ context.Context, tenantConfig model.TenantConfig, taskID string) error {
	taskKey := runnerTaskKey(tenantConfig.TenantID, taskID)
	r.mu.Lock()
	cancel, ok := r.cancels[taskKey]
	if ok {
		delete(r.cancels, taskKey)
	}
	r.mu.Unlock()
	if ok {
		cancel()
	}
	return nil
}

func (r *SnowflakeTaskRunner) forgetTask(taskKey string) {
	r.mu.Lock()
	delete(r.cancels, taskKey)
	r.mu.Unlock()
}

func (r *SnowflakeTaskRunner) buildConfig(tenantConfig model.TenantConfig, manifest model.TaskManifest) (snowflakeTaskConfig, error) {
	if manifest.Sink.Type != "" && manifest.Sink.Type != model.SinkTypeSnowflake {
		return snowflakeTaskConfig{}, fmt.Errorf("unsupported sink type %q", manifest.Sink.Type)
	}

	tidbConfig := mergeTiDBConfig(r.cfg.DefaultTiDB, tenantConfig.Source.TiDB, manifest.Source.TiDB)
	cdcConfig := mergeCDCConfig(r.cfg.DefaultCDC, tenantConfig.Source.CDC, manifest.Source.CDC)
	snowflakeConfig := mergeSnowflakeConfig(r.cfg.DefaultSnowflake, tenantConfig.Sink, manifest.Sink)
	storageRoot := firstNonEmptyString(manifest.Storage.URI, tenantConfig.StorageURI)
	awsCredentials, err := mergeAWSCredentials(r.cfg.DefaultAWSCredentials, tenantConfig.StorageCredentials, storageRoot)
	if err != nil {
		return snowflakeTaskConfig{}, err
	}

	storageURI, err := buildTaskStorageURI(storageRoot, manifest.TaskID, awsCredentials)
	if err != nil {
		return snowflakeTaskConfig{}, err
	}

	bindings := make([]model.TableBinding, len(manifest.Source.Tables))
	copy(bindings, manifest.Source.Tables)
	tables := make([]string, 0, len(bindings))
	for i := range bindings {
		if bindings[i].TargetDatabase == "" {
			bindings[i].TargetDatabase = firstNonEmptyString(manifest.Sink.Database, tenantConfig.Sink.Database, r.cfg.DefaultSnowflake.Database)
		}
		if bindings[i].TargetSchema == "" {
			bindings[i].TargetSchema = firstNonEmptyString(manifest.Sink.Schema, tenantConfig.Sink.Schema, r.cfg.DefaultSnowflake.Schema)
		}
		if bindings[i].TargetTable == "" {
			bindings[i].TargetTable = bindings[i].Table
		}
		tables = append(tables, fmt.Sprintf("%s.%s", bindings[i].Database, bindings[i].Table))
	}

	cdcFlushInterval := r.cfg.DefaultCDCFlushInterval
	if cdcFlushInterval == 0 {
		cdcFlushInterval = 60 * time.Second
	}
	if tenantConfig.Limits.CDCFlushInterval != "" {
		parsed, err := time.ParseDuration(tenantConfig.Limits.CDCFlushInterval)
		if err != nil {
			return snowflakeTaskConfig{}, err
		}
		cdcFlushInterval = parsed
	}
	if manifest.Limits.CDCFlushInterval != "" {
		parsed, err := time.ParseDuration(manifest.Limits.CDCFlushInterval)
		if err != nil {
			return snowflakeTaskConfig{}, err
		}
		cdcFlushInterval = parsed
	}

	snapshotConcurrency := firstNonZeroInt(r.cfg.DefaultSnapshotConcurrency, tenantConfig.Limits.SnapshotConcurrency, manifest.Limits.SnapshotConcurrency, 8)
	cdcFileSize := firstNonZeroInt(r.cfg.DefaultCDCFileSize, tenantConfig.Limits.CDCFileSize, manifest.Limits.CDCFileSize, 64*1024*1024)
	mode, err := taskModeToRunMode(manifest.Mode)
	if err != nil {
		return snowflakeTaskConfig{}, err
	}

	return snowflakeTaskConfig{
		tenantConfig:        tenantConfig,
		manifest:            manifest,
		tidbConfig:          tidbConfig,
		cdcConfig:           cdcConfig,
		snowflakeConfig:     snowflakeConfig,
		awsCredentials:      awsCredentials,
		storageURI:          storageURI,
		bindings:            bindings,
		tables:              tables,
		snapshotConcurrency: snapshotConcurrency,
		cdcFlushInterval:    cdcFlushInterval,
		cdcFileSize:         cdcFileSize,
		mode:                mode,
	}, nil
}

func (r *SnowflakeTaskRunner) validateSnowflakeTask(ctx context.Context, cfg snowflakeTaskConfig) error {
	if cfg.tidbConfig.Host == "" || cfg.tidbConfig.Port == 0 || cfg.tidbConfig.User == "" {
		return fmt.Errorf("tidb host, port, and user must be configured")
	}
	if cfg.cdcConfig.Host == "" || cfg.cdcConfig.Port == 0 {
		return fmt.Errorf("cdc host and port must be configured")
	}
	if cfg.snowflakeConfig.AccountId == "" || cfg.snowflakeConfig.Warehouse == "" || cfg.snowflakeConfig.User == "" ||
		cfg.snowflakeConfig.Pass == "" {
		return fmt.Errorf("snowflake account_id, warehouse, user, and pass must be configured")
	}

	db, err := cfg.tidbConfig.OpenDB()
	if err != nil {
		return err
	}
	defer db.Close()

	for _, binding := range cfg.bindings {
		columns, err := tidbsql.GetTiDBTableColumn(db, binding.Database, binding.Table)
		if err != nil {
			return err
		}
		if len(columns) == 0 {
			return fmt.Errorf("source table %s.%s does not exist or has no supported columns", binding.Database, binding.Table)
		}
		pkColumns, err := tidbsql.GetTiDBTablePKColumns(db, binding.Database, binding.Table)
		if err != nil {
			return err
		}
		if len(pkColumns) == 0 {
			return fmt.Errorf("source table %s.%s must have a primary key", binding.Database, binding.Table)
		}
		if binding.ColumnFilter.Mode != "" {
			if _, _, err := snowsql.ProjectSnowflakeColumns(columns, binding.ColumnFilter, pkColumns); err != nil {
				return err
			}
		}
	}
	_ = ctx
	return nil
}

func (r *SnowflakeTaskRunner) runSnowflakeTask(ctx context.Context, cfg snowflakeTaskConfig) error {
	snapshotURI, incrementURI, err := genSnapshotAndIncrementURIs(cfg.storageURI)
	if err != nil {
		return err
	}

	snapConnectorMap := make(map[string]coreinterfaces.Connector)
	increConnectorMap := make(map[string]coreinterfaces.Connector)
	for _, binding := range cfg.bindings {
		tableFQN := fmt.Sprintf("%s.%s", binding.Database, binding.Table)
		sfConfig := cfg.snowflakeConfig
		sfConfig.Database = firstNonEmptyString(binding.TargetDatabase, sfConfig.Database)
		sfConfig.Schema = firstNonEmptyString(binding.TargetSchema, sfConfig.Schema)
		options := snowsql.SnowflakeConnectorOptions{
			TargetTable:  binding.TargetTable,
			ColumnFilter: binding.ColumnFilter,
		}

		snapConnector, err := snowsql.NewSnowflakeConnectorWithOptions(
			&sfConfig,
			safeStageName("snapshot", cfg.tenantConfig.TenantID, cfg.manifest.TaskID, binding.Database, binding.Table),
			snapshotURI,
			&cfg.awsCredentials,
			options,
		)
		if err != nil {
			closeConnectors(snapConnectorMap)
			closeConnectors(increConnectorMap)
			return err
		}
		snapConnectorMap[tableFQN] = snapConnector

		increConnector, err := snowsql.NewSnowflakeConnectorWithOptions(
			&sfConfig,
			safeStageName("increment", cfg.tenantConfig.TenantID, cfg.manifest.TaskID, binding.Database, binding.Table),
			incrementURI,
			&cfg.awsCredentials,
			options,
		)
		if err != nil {
			closeConnectors(snapConnectorMap)
			closeConnectors(increConnectorMap)
			return err
		}
		increConnectorMap[tableFQN] = increConnector
	}
	defer closeConnectors(snapConnectorMap)
	defer closeConnectors(increConnectorMap)

	return ReplicateWithContext(
		ctx,
		&cfg.tidbConfig,
		cfg.tables,
		cfg.storageURI,
		snapshotURI,
		incrementURI,
		cfg.snapshotConcurrency,
		cfg.cdcConfig.Host,
		cfg.cdcConfig.Port,
		cfg.cdcFlushInterval,
		cfg.cdcFileSize,
		snapConnectorMap,
		increConnectorMap,
		"snowflake",
		true,
		cfg.mode,
	)
}

func buildTaskStorageURI(storageRoot string, taskID string, cred credentials.Value) (*url.URL, error) {
	uri, err := buildStorageURIWithCredentials(storageRoot, cred)
	if err != nil {
		return nil, err
	}
	uri.Path = path.Join(uri.Path, "data", "tasks", taskID)
	return uri, nil
}

func buildStorageURIWithCredentials(storageRoot string, cred credentials.Value) (*url.URL, error) {
	uri, err := url.Parse(storageRoot)
	if err != nil {
		return nil, err
	}
	if uri.Scheme != "s3" {
		return nil, fmt.Errorf("tenant task storage only supports s3")
	}
	values := uri.Query()
	if cred.AccessKeyID != "" {
		values.Set("access-key", cred.AccessKeyID)
	}
	if cred.SecretAccessKey != "" {
		values.Set("secret-access-key", cred.SecretAccessKey)
	}
	if cred.SessionToken != "" {
		values.Set("session-token", cred.SessionToken)
	}
	uri.RawQuery = values.Encode()
	return uri, nil
}

func mergeTiDBConfig(base tidbsql.TiDBConfig, tenant model.SourceTiDBConfig, task model.SourceTiDBConfig) tidbsql.TiDBConfig {
	cfg := base
	if tenant.Host != "" {
		cfg.Host = tenant.Host
	}
	if tenant.Port != 0 {
		cfg.Port = tenant.Port
	}
	if tenant.User != "" {
		cfg.User = tenant.User
	}
	if tenant.Pass != "" {
		cfg.Pass = tenant.Pass
	}
	if tenant.SSLCA != "" {
		cfg.SSLCA = tenant.SSLCA
	}
	if task.Host != "" {
		cfg.Host = task.Host
	}
	if task.Port != 0 {
		cfg.Port = task.Port
	}
	if task.User != "" {
		cfg.User = task.User
	}
	if task.Pass != "" {
		cfg.Pass = task.Pass
	}
	if task.SSLCA != "" {
		cfg.SSLCA = task.SSLCA
	}
	return cfg
}

func mergeCDCConfig(base model.SourceCDCConfig, tenant model.SourceCDCConfig, task model.SourceCDCConfig) model.SourceCDCConfig {
	cfg := base
	if tenant.Host != "" {
		cfg.Host = tenant.Host
	}
	if tenant.Port != 0 {
		cfg.Port = tenant.Port
	}
	if task.Host != "" {
		cfg.Host = task.Host
	}
	if task.Port != 0 {
		cfg.Port = task.Port
	}
	return cfg
}

func mergeSnowflakeConfig(base snowsql.SnowflakeConfig, tenant model.SinkConfig, task model.SinkConfig) snowsql.SnowflakeConfig {
	cfg := base
	applySinkConfig := func(sink model.SinkConfig) {
		if sink.AccountID != "" {
			cfg.AccountId = sink.AccountID
		}
		if sink.Warehouse != "" {
			cfg.Warehouse = sink.Warehouse
		}
		if sink.User != "" {
			cfg.User = sink.User
		}
		if sink.Pass != "" {
			cfg.Pass = sink.Pass
		}
		if sink.Database != "" {
			cfg.Database = sink.Database
		}
		if sink.Schema != "" {
			cfg.Schema = sink.Schema
		}
	}
	applySinkConfig(tenant)
	applySinkConfig(task)
	return cfg
}

func mergeAWSCredentials(base credentials.Value, tenant model.AWSCredentials, storageRoots ...string) (credentials.Value, error) {
	if tenant.AccessKeyID != "" || tenant.SecretAccessKey != "" || tenant.SessionToken != "" {
		return credentials.Value{
			AccessKeyID:     tenant.AccessKeyID,
			SecretAccessKey: tenant.SecretAccessKey,
			SessionToken:    tenant.SessionToken,
		}, nil
	}
	if base.AccessKeyID != "" || base.SecretAccessKey != "" || base.SessionToken != "" {
		return base, nil
	}
	for _, storageRoot := range storageRoots {
		uri, err := url.Parse(storageRoot)
		if err != nil {
			return credentials.Value{}, err
		}
		query := uri.Query()
		if query.Get("access-key") != "" || query.Get("secret-access-key") != "" || query.Get("session-token") != "" {
			return credentials.Value{
				AccessKeyID:     query.Get("access-key"),
				SecretAccessKey: query.Get("secret-access-key"),
				SessionToken:    query.Get("session-token"),
			}, nil
		}
	}
	credValue, err := credentials.NewEnvCredentials().Get()
	if err != nil {
		return credentials.Value{}, err
	}
	return credValue, nil
}

func taskModeToRunMode(mode model.TaskMode) (RunMode, error) {
	switch mode {
	case "", model.TaskModeFull:
		return RunModeFull, nil
	case model.TaskModeSnapshotOnly:
		return RunModeSnapshotOnly, nil
	case model.TaskModeIncrementalOnly:
		return RunModeIncrementalOnly, nil
	default:
		return RunModeFull, fmt.Errorf("unsupported task mode %q", mode)
	}
}

func safeStageName(parts ...string) string {
	joined := strings.Join(parts, "_")
	var sb strings.Builder
	for _, r := range joined {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			sb.WriteRune(r)
			continue
		}
		sb.WriteByte('_')
	}
	return sb.String()
}

func closeConnectors(connectors map[string]coreinterfaces.Connector) {
	for _, connector := range connectors {
		connector.Close()
	}
}

func runnerTaskKey(tenantID, taskID string) string {
	return tenantID + "/" + taskID
}

func firstNonZeroInt(values ...int) int {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
