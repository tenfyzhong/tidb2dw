package cmd

import (
	"sync"

	"github.com/pingcap/errors"
)

const DefaultTableConcurrency = 32

func runTableWorkers(tables []string, concurrency int, fn func(table string) error) error {
	if concurrency <= 0 {
		return errors.Errorf("table concurrency must be positive, got %d", concurrency)
	}
	if fn == nil {
		return errors.New("table worker function must not be nil")
	}
	if len(tables) == 0 {
		return nil
	}

	workerSlots := make(chan struct{}, concurrency)
	errCh := make(chan error, len(tables))
	var wg sync.WaitGroup

	for _, table := range tables {
		table := table
		workerSlots <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() {
				<-workerSlots
				wg.Done()
			}()
			if err := fn(table); err != nil {
				errCh <- errors.Annotatef(err, "table %s", table)
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		return errors.Trace(err)
	}
	return nil
}
