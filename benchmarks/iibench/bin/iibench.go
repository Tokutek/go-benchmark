package main

import (
	"flag"
	"github.com/Tokutek/go-benchmark"
	"github.com/Tokutek/go-benchmark/benchmarks/iibench"
	"github.com/Tokutek/go-benchmark/mongotools"
	"labix.org/v2/mgo"
	"log"
	"time"
)

var (
	host           = flag.String("host", "localhost", "host:port string of database to connect to")
	dbname         = flag.String("db", "iibench", "dbname")
	collname       = flag.String("coll", "purchases_index", "collname")
	numCollections = flag.Int("numCollections", 1, "number of collections to simultaneously run on")

	// for benchmark
	numWriters          = flag.Int("numWriterThreads", 1, "specify the number of writer threads")
	numQueryThreads     = flag.Int("numQueryThreads", 0, "specify the number of threads to perform queries")
	numSeconds          = flag.Int64("numSeconds", 5, "number of seconds the benchmark is to run. If this value is > 0, then numInsertsPerThread MUST be 0, and vice versa")
	numInsertsPerThread = flag.Uint64("numInsertsPerThread", 0, "number of inserts to be done per thread. If this value is > 0, then numSeconds MUST be 0 and numQueryThreads MUST be 0")
)

func main() {
	flag.Parse()

	if *numInsertsPerThread > 0 && (*numQueryThreads > 0 || *numSeconds > 0) {
		log.Fatal("Invalid values for numInsertsPerThread: ", *numInsertsPerThread, ", numQueryThreads: ", *numQueryThreads, ", numSeconds: ", *numSeconds)
	}

	session, err := mgo.Dial(*host)
	if err != nil {
		log.Fatal("Error connecting to ", *host, ": ", err)
	}
	// so we are not in fire and forget
	session.SetSafe(&mgo.Safe{})
	defer session.Close()

	indexes := make([]mgo.Index, 3)
	indexes[0] = mgo.Index{Key: []string{"pr", "cid"}}
	indexes[1] = mgo.Index{Key: []string{"crid", "pr", "cid"}}
	indexes[2] = mgo.Index{Key: []string{"pr", "ts", "cid"}}

	mongotools.MakeCollections(*collname, *dbname, *numCollections, session, indexes)
	// at this point we have created the collection, now run the benchmark
	res := new(iibench.Result)
	workers := make([]benchmark.WorkInfo, 0, *numWriters+*numQueryThreads)
	for i := 0; i < *numWriters; i++ {
		copiedSession := session.Copy()
		defer copiedSession.Close()
		var gen = iibench.NewDocGenerator()
		currCollectionString := mongotools.GetCollectionString(*collname, i%*numCollections)
		workers = append(workers, mongotools.MakeCollectionWriter(gen, copiedSession, *dbname, currCollectionString, *numInsertsPerThread))
	}
	for i := 0; i < *numQueryThreads; i++ {
		currCollectionString := mongotools.GetCollectionString(*collname, i%*numCollections)
		copiedSession := session.Copy()
		defer copiedSession.Close()
		workers = append(workers, iibench.NewQueryWork(copiedSession, *dbname, currCollectionString))
	}
	benchmark.Run(res, workers, time.Duration(*numSeconds)*time.Second)
}
