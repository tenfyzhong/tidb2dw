package cmd

import (
	"net/url"
	"reflect"
	"testing"
)

func TestBuildSysbenchTables(t *testing.T) {
	tables, err := buildSysbenchTables("sbtest", "sbtest", 3)
	if err != nil {
		t.Fatalf("buildSysbenchTables returned error: %v", err)
	}

	expected := []string{"sbtest.sbtest1", "sbtest.sbtest2", "sbtest.sbtest3"}
	if !reflect.DeepEqual(tables, expected) {
		t.Fatalf("tables = %v, want %v", tables, expected)
	}
}

func TestBuildSysbenchTablesRejectsInvalidConfig(t *testing.T) {
	testCases := []struct {
		name   string
		db     string
		prefix string
		count  int
	}{
		{
			name:   "missing database",
			db:     "",
			prefix: "sbtest",
			count:  1,
		},
		{
			name:   "missing prefix",
			db:     "sbtest",
			prefix: "",
			count:  1,
		},
		{
			name:   "zero count",
			db:     "sbtest",
			prefix: "sbtest",
			count:  0,
		},
		{
			name:   "negative count",
			db:     "sbtest",
			prefix: "sbtest",
			count:  -1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := buildSysbenchTables(tc.db, tc.prefix, tc.count); err == nil {
				t.Fatal("buildSysbenchTables returned nil error")
			}
		})
	}
}

func TestGenTableScopedStorageURI(t *testing.T) {
	baseURI, err := url.Parse("s3://example-bucket/export-root?access-key=ak&secret-access-key=sk")
	if err != nil {
		t.Fatalf("parse base URI: %v", err)
	}

	got, err := genTableScopedStorageURI(baseURI, "sbtest.sbtest1")
	if err != nil {
		t.Fatalf("genTableScopedStorageURI returned error: %v", err)
	}

	expected := "s3://example-bucket/export-root/sbtest.sbtest1?access-key=ak&secret-access-key=sk"
	if got.String() != expected {
		t.Fatalf("URI = %s, want %s", got.String(), expected)
	}
	if baseURI.String() != "s3://example-bucket/export-root?access-key=ak&secret-access-key=sk" {
		t.Fatalf("base URI was mutated: %s", baseURI.String())
	}
}

func TestGenTableScopedReplicationURIs(t *testing.T) {
	baseURI, err := url.Parse("s3://example-bucket/export-root?access-key=ak&secret-access-key=sk")
	if err != nil {
		t.Fatalf("parse base URI: %v", err)
	}

	got, err := genTableScopedReplicationURIs(baseURI, "sbtest.sbtest1")
	if err != nil {
		t.Fatalf("genTableScopedReplicationURIs returned error: %v", err)
	}

	expectedStorageURI := "s3://example-bucket/export-root/sbtest.sbtest1?access-key=ak&secret-access-key=sk"
	if got.storageURI.String() != expectedStorageURI {
		t.Fatalf("storage URI = %s, want %s", got.storageURI.String(), expectedStorageURI)
	}
	expectedSnapshotURI := "s3://example-bucket/export-root/sbtest.sbtest1/snapshot?access-key=ak&secret-access-key=sk"
	if got.snapshotURI.String() != expectedSnapshotURI {
		t.Fatalf("snapshot URI = %s, want %s", got.snapshotURI.String(), expectedSnapshotURI)
	}
	expectedIncrementURI := "s3://example-bucket/export-root/sbtest.sbtest1/increment?access-key=ak&secret-access-key=sk"
	if got.incrementURI.String() != expectedIncrementURI {
		t.Fatalf("increment URI = %s, want %s", got.incrementURI.String(), expectedIncrementURI)
	}
}
