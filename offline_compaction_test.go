// +build integration

/*
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package badger

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/options"
	"github.com/dgraph-io/badger/y"
	"github.com/paulbellamy/ratecounter"
)

var (
	numKeys   = flag.Float64("keys_mil", 10.0, "How many million keys to write.")
	valueSize = flag.Int("valsz", 128, "Value size in bytes.")
	dir       = flag.String("dir", "", "Base dir for writes.")
	skipLoad  = flag.Bool("skipLoad", false, "Flag to skip loading and run delete/compaction only")
	bdb       *DB
)

const mil float64 = 1000000

func TestOfflineCompaction(t *testing.T) {
	if *skipLoad {
		fmt.Printf("Using existing db at: %s\n", *dir)
	} else {
		openDB(true)
		fmt.Printf("TOTAL KEYS TO WRITE: %s\n", humanize(int64(*numKeys*mil)))
		loadData()
		// TODO print some stats (maybe run badger_info)
	}

	openDB(false)
	fmt.Print("Deleting some keys\n")
	// Delete Some Data

	fmt.Println("Purging Older Versions")
	//bdb.PurgeOlderVersions()
	fmt.Println("Running Value Log GC")
	//bdb.RunValueLogGCOffline()
	fmt.Println("Running offline compaction")
	//bdb.CompactLSMTreeOffline()

	// TODO print some stats (maybe run badger_info)
}

func openDB(remove bool) {
	opt := DefaultOptions
	opt.TableLoadingMode = options.MemoryMap
	opt.Dir = *dir + "/badger"
	opt.ValueDir = opt.Dir
	opt.SyncWrites = true

	// Open DB
	if remove {
		fmt.Println("Init Badger DB")
		y.Check(os.RemoveAll(*dir + "/badger"))
	} else {
		fmt.Println("Opening Badger DB")
	}
	os.MkdirAll(*dir+"/badger", 0777)
	var err error
	bdb, err = Open(opt)
	if err != nil {
		log.Fatalf("while opening badger: %v", err)
	}
}

func loadData() {
	rc := ratecounter.NewRateCounter(time.Minute)
	var counter int64
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		var count int64
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				fmt.Printf("[%04d] Write key rate per minute: %s. Total: %s\n",
					count,
					humanize(rc.Rate()),
					humanize(atomic.LoadInt64(&counter)))
				count++
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		if err := http.ListenAndServe("0.0.0.0:8081", nil); err != nil {
			log.Fatalf("While opening http. Error: %v", err)
		}
	}()

	N := 12
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(proc int) {
			entries := make([]*entry, 1000)
			for i := 0; i < len(entries); i++ {
				e := new(entry)
				e.Key = make([]byte, 22)
				e.Value = make([]byte, *valueSize)
				entries[i] = e
			}

			var written float64
			for written < (*numKeys*mil)/float64(N) {
				wrote := float64(writeBatch(entries))

				wi := int64(wrote)
				atomic.AddInt64(&counter, wi)
				rc.Incr(wi)

				written += wrote
			}
			wg.Done()
		}(i)
	}
	// 	wg.Add(1) // Block
	wg.Wait()
	cancel()
}

type entry struct {
	Key   []byte
	Value []byte
	Meta  byte
}

func fillEntry(e *entry) {
	k := rand.Int() % int(*numKeys*mil)
	key := fmt.Sprintf("vsz=%05d-k=%010d", *valueSize, k) // 22 bytes.
	if cap(e.Key) < len(key) {
		e.Key = make([]byte, 2*len(key))
	}
	e.Key = e.Key[:len(key)]
	copy(e.Key, key)

	rand.Read(e.Value)
	e.Meta = 0
}

func writeBatch(entries []*entry) int {
	for _, e := range entries {
		fillEntry(e)
	}

	txn := bdb.NewTransaction(true)

	for _, e := range entries {
		y.Check(txn.Set(e.Key, e.Value))
	}
	y.Check(txn.Commit(nil))

	return len(entries)
}

func humanize(n int64) string {
	if n >= 1000000 {
		return fmt.Sprintf("%6.2fM", float64(n)/1000000.0)
	}
	if n >= 1000 {
		return fmt.Sprintf("%6.2fK", float64(n)/1000.0)
	}
	return fmt.Sprintf("%5.2f", float64(n))
}
