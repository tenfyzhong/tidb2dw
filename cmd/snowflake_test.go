package cmd

import "testing"

func TestSnowflakeCommandHasSysbenchFlags(t *testing.T) {
	cmd := NewSnowflakeCmd()

	for _, flagName := range []string{"sysbench.database", "sysbench.table-prefix", "sysbench.tables", "table-concurrency", "snapshot-table-concurrency"} {
		if cmd.Flags().Lookup(flagName) == nil {
			t.Fatalf("missing flag %q", flagName)
		}
	}
}

func TestSnowflakeCommandSetsDefaultTableConcurrency(t *testing.T) {
	cmd := NewSnowflakeCmd()
	flag := cmd.Flags().Lookup("table-concurrency")
	if flag == nil {
		t.Fatal("missing table-concurrency flag")
	}
	if flag.DefValue != "32" {
		t.Fatalf("table-concurrency default = %s, want 32", flag.DefValue)
	}
}

func TestSnowflakeCommandSetsDefaultSnapshotTableConcurrency(t *testing.T) {
	cmd := NewSnowflakeCmd()
	flag := cmd.Flags().Lookup("snapshot-table-concurrency")
	if flag == nil {
		t.Fatal("missing snapshot-table-concurrency flag")
	}
	if flag.DefValue != "4" {
		t.Fatalf("snapshot-table-concurrency default = %s, want 4", flag.DefValue)
	}
}
