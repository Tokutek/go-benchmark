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
	coll          *mgo.Collection
	docsPerInsert int
	gen           DocGenerator
}

func (w *insertWork) Do(c chan benchmark.Stats) {
	numInserted := 0
	docs := make([]interface{}, w.docsPerInsert)
	// if docsPerInsert is less than 50, we want
	// to batch the operations before sending it over a channel
	// This is an attempt to get 10% back from iibench
	// when docsPerInsert=1
	for numInserted < minBatchSizeForChannel {
		for i := 0; i < len(docs); i++ {
			docs[i] = w.gen.Generate()
		}
		err := w.coll.Insert(docs...)
		if err != nil {
			log.Print("received error ", err)
		}
		numInserted += len(docs)
	}
	c <- benchmark.Stats{Inserts: uint64(numInserted)}
}

func (w *insertWork) Close() {
}

// returns a WorkInfo that can be used for loading documents into a collection
// This is essentially a helper function for the purpose of loading data into collections,
// be it an iibench writer or a sysbench trickle loader. The caller defines how to generate
// the documents, via the DocGenerator passed in, and how many insertions the WorkInfo is to
// do (with 0 meaning unlimited and that the benchmark is bounded by time), and a WorkInfo is returned
// This file exports flags "docsPerInsert" that defines the batching of the writer, "insertsPerInterval" and "insertInterval"
// to define whether there should be any gating.
func MakeCollectionWriter(gen DocGenerator, session *mgo.Session, dbname string, collname string, numInsertsPerThread int) benchmark.WorkInfo {
	db := session.DB(dbname)
	coll := db.C(collname)
	writer := &insertWork{
		coll,
		*docsPerInsert,
		gen}
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
