package cmd

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestRunTableWorkersHonorsConcurrency(t *testing.T) {
	tables := []string{"t1", "t2", "t3", "t4", "t5"}
	var active int32
	var maxActive int32

	err := runTableWorkers(tables, 2, func(_ string) error {
		current := atomic.AddInt32(&active, 1)
		for {
			max := atomic.LoadInt32(&maxActive)
			if current <= max || atomic.CompareAndSwapInt32(&maxActive, max, current) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		atomic.AddInt32(&active, -1)
		return nil
	})
	if err != nil {
		t.Fatalf("runTableWorkers returned error: %v", err)
	}
	if maxActive != 2 {
		t.Fatalf("max active workers = %d, want 2", maxActive)
	}
}

func TestRunTableWorkersRejectsInvalidConcurrency(t *testing.T) {
	if err := runTableWorkers([]string{"t1"}, 0, func(_ string) error { return nil }); err == nil {
		t.Fatal("runTableWorkers returned nil error")
	}
}
