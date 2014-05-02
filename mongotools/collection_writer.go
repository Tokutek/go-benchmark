package mongotools

import (
	"flag"
	"github.com/Tokutek/go-benchmark"
	"labix.org/v2/mgo"
	"log"
)

var (
	docsPerInsert      = flag.Int("docsPerInsert", 10, "specify the number of documents per insert")
	insertsPerInterval = flag.Int("insertsPerInterval", 0, "max inserts per interval, 0 means unlimited")
	insertInterval     = flag.Int("insertInterval", 1, "interval for inserts, in seconds, meant to be used with -insertsPerInterval")
)

var minBatchSizeForChannel = 50

type DocGenerator interface {
	Generate() interface{}
}

// implements Work
type insertWork struct {
	coll *mgo.Collection
	ch   <-chan []interface{}
	kill chan<- bool
}

func (w *insertWork) Do(c chan<- interface{}) {
	numInserted := 0
	// if docsPerInsert is less than 50, we want
	// to batch the operations before sending it over a channel
	// This is an attempt to get 10% back from iibench
	// when docsPerInsert=1
	for numInserted < minBatchSizeForChannel {
		docs := <-w.ch
		err := w.coll.Insert(docs...)
		if err != nil {
			log.Print("received error ", err)
		}
		numInserted += len(docs)
	}
	c <- benchmark.Stats{Inserts: uint64(numInserted)}
}

func (w *insertWork) Close() {
	w.kill <- true
	close(w.kill)
}

// returns a WorkInfo that can be used for loading documents into a collection
// This is essentially a helper function for the purpose of loading data into collections,
// be it an iibench writer or a sysbench trickle loader. The caller defines how to generate
// the documents, via the DocGenerator passed in, and how many insertions the WorkInfo is to
// do (with 0 meaning unlimited and that the benchmark is bounded by time), and a WorkInfo is returned
// This file exports flags "docsPerInsert" that defines the batching of the writer, "insertsPerInterval" and "insertInterval"
// to define whether there should be any gating.
func NewInsertWork(gen DocGenerator, coll *mgo.Collection, numInsertsPerThread int) benchmark.WorkInfo {
	kill := make(chan bool)
	ch := make(chan []interface{}, 10)
	go func() {
		defer close(ch)
		dpi := *docsPerInsert
		for {
			docs := make([]interface{}, dpi)
			for i := range docs {
				docs[i] = gen.Generate()
			}
			select {
			case <-kill:
				return
			case ch <- docs:
			}
		}
	}()
	writer := &insertWork{coll, ch, kill}
	var (
		numOps         int
		opsPerInterval int
	)
	if *insertsPerInterval > 0 && *insertsPerInterval < minBatchSizeForChannel {
		minBatchSizeForChannel = *insertsPerInterval
	}
	if numInsertsPerThread > 0 && numInsertsPerThread < minBatchSizeForChannel {
		minBatchSizeForChannel = numInsertsPerThread
	}
	if *docsPerInsert < minBatchSizeForChannel && minBatchSizeForChannel%*docsPerInsert != 0 {
		log.Fatal("If you want DocsPerInterval < ", minBatchSizeForChannel, ", make it divisible by ", minBatchSizeForChannel)
	}
	if *docsPerInsert < minBatchSizeForChannel {
		numOps = numInsertsPerThread / minBatchSizeForChannel
		opsPerInterval = *insertsPerInterval / minBatchSizeForChannel
	} else {
		numOps = numInsertsPerThread / *docsPerInsert
		opsPerInterval = *insertsPerInterval / *docsPerInsert
	}
	log.Println("opsPerInterval ", opsPerInterval, " numOps ", numOps)
	workInfo := benchmark.WorkInfo{writer, uint64(opsPerInterval), uint64(*insertInterval), uint64(numOps)}
	return workInfo
}
