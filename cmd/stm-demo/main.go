// Command stm-demo runs a fixed deterministic workload against the STM store,
// then a second process ("restart") recovers state from the WAL and prints
// it. Run twice with the same data dir to observe crash recovery.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/guoyyit/software-transactional-memory-go/stm"
)

func main() {
	dir := flag.String("dir", ".stm-demo", "data directory")
	flag.Parse()

	if err := os.MkdirAll(*dir, 0o755); err != nil {
		panic(err)
	}
	cfg := stm.DefaultConfig()
	cfg.WALPath = filepath.Join(*dir, "txn.log")

	s, err := stm.New(cfg)
	if err != nil {
		panic(err)
	}

	// Increment shared counters concurrently; the STM serializes conflicts.
	for i := 0; i < 5; i++ {
		if err := s.Atomically(func(tx *stm.Tx) error {
			b, err := tx.Get("counter")
			if errors.Is(err, stm.ErrNotFound) {
				b = []byte("0")
			} else if err != nil {
				return err
			}
			n, _ := strconv.Atoi(string(b))
			time.Sleep(time.Millisecond) // pretend work
			return tx.Put("counter", []byte(strconv.Itoa(n+1)))
		}); err != nil {
			panic(err)
		}
	}

	var counter string
	_ = s.Atomically(func(tx *stm.Tx) error {
		b, err := tx.Get("counter")
		if err != nil {
			return err
		}
		counter = string(b)
		return nil
	})
	if err := s.Close(); err != nil {
		panic(err)
	}
	fmt.Printf("counter after run: %s (restart reuses %s)\n", counter, cfg.WALPath)
}
