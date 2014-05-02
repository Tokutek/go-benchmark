package mongotools

import (
	"flag"
	//"fmt"
	"github.com/Tokutek/go-benchmark"
	"labix.org/v2/mgo"
	"log"
)

var docsPerInsert *uint64 = flag.Uint64("docsPerInsert", 10, "specify the number of documents per insert")
var insertsPerInterval *uint64 = flag.Uint64("insertsPerInterval", 0, "max inserts per interval, 0 means unlimited")
var insertInterval *uint64 = flag.Uint64("insertInterval", 1, "interval for inserts, in seconds, meant to be used with -insertsPerInterval")

var minBatchSizeForChannel uint64 = 50

// interface passed into MakeCollectionWriter that is used to generate
// documents for insertion
type DocGenerator interface {
	Generate() interface{}
}

// implements Work
type collectionWriter struct {
	Session       *mgo.Session
	Coll          *mgo.Collection
	DocsPerInsert uint64
	Gen           DocGenerator
}

func (w collectionWriter) Do(c chan benchmark.Stats) {
	var numInserted uint64
	docs := make([]interface{}, w.DocsPerInsert)
	// if docsPerInsert is less than 50, we want
	// to batch the operations before sending it over a channel
	// This is an attempt to get 10% back from iibench
	// when docsPerInsert=1
	for numInserted < minBatchSizeForChannel {
		for i := 0; i < len(docs); i++ {
			docs[i] = w.Gen.Generate()
		}
		err := w.Coll.Insert(docs...)
		if err != nil {
			log.Print("received error ", err)
		}
		numInserted += w.DocsPerInsert
	}
	c <- benchmark.Stats{Inserts: numInserted}
}

func (w collectionWriter) Close() {
	w.Session.Close()
}

// returns a WorkInfo that can be used for loading documents into a collection
// This is essentially a helper function for the purpose of loading data into collections,
// be it an iibench writer or a sysbench trickle loader. The caller defines how to generate
// the documents, via the DocGenerator passed in, and how many insertions the WorkInfo is to
// do (with 0 meaning unlimited and that the benchmark is bounded by time), and a WorkInfo is returned
// This file exports flags "docsPerInsert" that defines the batching of the writer, "insertsPerInterval" and "insertInterval"
// to define whether there should be any gating.
func MakeCollectionWriter(gen DocGenerator, session *mgo.Session, dbname string, collname string, numInsertsPerThread uint64) benchmark.WorkInfo {
	copiedSession := session.Copy()
	copiedSession.SetSafe(&mgo.Safe{})
	db := copiedSession.DB(dbname)
	coll := db.C(collname)
	writer := collectionWriter{
		copiedSession,
		coll,
		*docsPerInsert,
		gen}
	var numOps uint64
	var opsPerInterval uint64
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
	workInfo := benchmark.WorkInfo{writer, opsPerInterval, *insertInterval, numOps}
	return workInfo
}
