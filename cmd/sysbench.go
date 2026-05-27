package cmd

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/pingcap/errors"
)

type tableScopedReplicationURIs struct {
	storageURI   *url.URL
	snapshotURI  *url.URL
	incrementURI *url.URL
}

func buildSysbenchTables(database, tablePrefix string, tableCount int) ([]string, error) {
	database = strings.TrimSpace(database)
	tablePrefix = strings.TrimSpace(tablePrefix)
	if database == "" {
		return nil, errors.New("sysbench database must not be empty")
	}
	if tablePrefix == "" {
		return nil, errors.New("sysbench table prefix must not be empty")
	}
	if tableCount <= 0 {
		return nil, errors.Errorf("sysbench table count must be positive, got %d", tableCount)
	}

	tables := make([]string, 0, tableCount)
	for i := 1; i <= tableCount; i++ {
		tables = append(tables, fmt.Sprintf("%s.%s%d", database, tablePrefix, i))
	}
	return tables, nil
}

func genTableScopedStorageURI(storageURI *url.URL, tableFQN string) (*url.URL, error) {
	if storageURI == nil {
		return nil, errors.New("storage URI must not be nil")
	}
	tableFQN = strings.TrimSpace(tableFQN)
	if tableFQN == "" {
		return nil, errors.New("table name must not be empty")
	}

	tableStorageURI := *storageURI
	tablePath, err := url.JoinPath(storageURI.Path, tableFQN)
	if err != nil {
		return nil, errors.Annotate(err, "failed to join table storage path")
	}
	tableStorageURI.Path = tablePath
	return &tableStorageURI, nil
}

func genTableScopedReplicationURIs(storageURI *url.URL, tableFQN string) (*tableScopedReplicationURIs, error) {
	tableStorageURI, err := genTableScopedStorageURI(storageURI, tableFQN)
	if err != nil {
		return nil, errors.Trace(err)
	}
	snapshotURI, incrementURI, err := genSnapshotAndIncrementURIs(tableStorageURI)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &tableScopedReplicationURIs{
		storageURI:   tableStorageURI,
		snapshotURI:  snapshotURI,
		incrementURI: incrementURI,
	}, nil
}
