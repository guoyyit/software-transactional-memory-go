package stm_test

import (
	"encoding/binary"
	"fmt"

	"github.com/guoyyit/software-transactional-memory-go/stm"
)

func Example() {
	db := stm.New()
	defer db.Close()

	// Atomic counter increment under contention-safe retry.
	incr := func(key string) error {
		return db.Do(func(tx *stm.Txn) error {
			var n uint64
			if v, ok := tx.GetOK(key); ok {
				n = binary.LittleEndian.Uint64(v)
			}
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], n+1)
			tx.Set(key, buf[:])
			return nil
		})
	}

	for i := 0; i < 5; i++ {
		if err := incr("counter"); err != nil {
			panic(err)
		}
	}

	var got uint64
	if err := db.Do(func(tx *stm.Txn) error {
		v, err := tx.Get("counter")
		if err != nil {
			return err
		}
		got = binary.LittleEndian.Uint64(v)
		return nil
	}); err != nil {
		panic(err)
	}
	fmt.Println(got)
	// Output: 5
}

func ExampleTxn_Retry() {
	db := stm.New()
	defer db.Close()

	done := make(chan struct{})
	go func() {
		// Wait until "ready" exists, retrying conditionally.
		_ = db.Do(func(tx *stm.Txn) error {
			if _, ok := tx.GetOK("ready"); !ok {
				tx.Retry() // park until another transaction commits
			}
			close(done)
			return nil
		})
	}()

	_ = db.Do(func(tx *stm.Txn) error {
		tx.Set("ready", []byte("go"))
		return nil
	})
	<-done
	fmt.Println("ready")
	// Output: ready
}
