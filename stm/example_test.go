package stm_test

import (
	"errors"
	"fmt"
	"log"
	"strconv"

	"github.com/guoyyit/software-transactional-memory-go/stm"
)

func ExampleStore_Atomically() {
	s, err := stm.New(stm.DefaultConfig())
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	// Atomic increment: read-modify-write, conflicts auto-retry.
	increment := func(key string) error {
		return s.Atomically(func(tx *stm.Tx) error {
			b, err := tx.Get(key)
			if errors.Is(err, stm.ErrNotFound) {
				b = []byte("0")
			} else if err != nil {
				return err
			}
			n, _ := strconv.Atoi(string(b))
			return tx.Put(key, []byte(strconv.Itoa(n+1)))
		})
	}
	for i := 0; i < 3; i++ {
		if err := increment("counter"); err != nil {
			log.Fatal(err)
		}
	}

	var v string
	_ = s.Atomically(func(tx *stm.Tx) error {
		b, err := tx.Get("counter")
		if err != nil {
			return err
		}
		v = string(b)
		return nil
	})
	fmt.Println(v)
	// Output: 3
}

func ExampleStore_conditionalRetry() {
	s, _ := stm.New(stm.DefaultConfig())
	defer s.Close()

	go func() {
		_ = s.Atomically(func(tx *stm.Tx) error { return tx.Put("ready", []byte("go")) })
	}()

	// Wait until a condition holds without polling.
	_ = s.Atomically(func(tx *stm.Tx) error {
		if _, err := tx.Get("ready"); errors.Is(err, stm.ErrNotFound) {
			return tx.Retry()
		}
		return nil
	})
	fmt.Println("ready observed")
	// Output: ready observed
}
