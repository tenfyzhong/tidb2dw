package cmd

import "testing"

func TestSnowflakeCommandHasSysbenchFlags(t *testing.T) {
	cmd := NewSnowflakeCmd()

	for _, flagName := range []string{"sysbench.database", "sysbench.table-prefix", "sysbench.tables", "table-concurrency"} {
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
