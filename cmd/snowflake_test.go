package cmd

import "testing"

func TestSnowflakeCommandHasSysbenchFlags(t *testing.T) {
	cmd := NewSnowflakeCmd()

	for _, flagName := range []string{"sysbench.database", "sysbench.table-prefix", "sysbench.tables"} {
		if cmd.Flags().Lookup(flagName) == nil {
			t.Fatalf("missing flag %q", flagName)
		}
	}
}
