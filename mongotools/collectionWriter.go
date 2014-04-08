package mongotools

import (
	"flag"
	//"fmt"
	"github.com/zkasheff/tokubenchmark"
	"labix.org/v2/mgo"
	"log"
)

var docsPerInsert *uint64 = flag.Uint64("docsPerInsert", 10, "specify the number of documents per insert")
var insertsPerInterval *uint64 = flag.Uint64("insertsPerInterval", 0, "max inserts per interval, 0 means unlimited")
var insertInterval *uint64 = flag.Uint64("insertInterval", 1, "interval for inserts, in seconds, meant to be used with -insertsPerInterval")

var minBatchSizeForChannel uint64 = 50

type DocGenerator interface {
	MakeDoc() interface{}
}

// implements BenchmarkWorkItem
type collectionWriter struct {
	Session       *mgo.Session
	Coll          *mgo.Collection
	DocsPerInsert uint64
	Gen           DocGenerator
}

func (w collectionWriter) DoWork(c chan tokubenchmark.BenchmarkStats) {
	var numInserted uint64
	docs := make([]interface{}, w.DocsPerInsert)
	// if docsPerInsert is less than 50, we want
	// to batch the operations before sending it over a channel
	// This is an attempt to get 10% back from iibench
	// when docsPerInsert=1
	for numInserted < minBatchSizeForChannel {
		for i := 0; i < len(docs); i++ {
			docs[i] = w.Gen.MakeDoc()
		}
		err := w.Coll.Insert(docs...)
		if err != nil {
			log.Print("received error ", err)
		}
		numInserted += w.DocsPerInsert
	}
	c <- tokubenchmark.BenchmarkStats{Inserts: numInserted}
}

func (w collectionWriter) Close() {
	w.Session.Close()
}

func MakeCollectionWriter(gen DocGenerator, session *mgo.Session, dbname string, collname string, numInsertsPerThread uint64) tokubenchmark.BenchmarkWorkInfo {
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
	workInfo := tokubenchmark.BenchmarkWorkInfo{writer, opsPerInterval, *insertInterval, numOps}
	return workInfo
}
