package main

import (
	"flag"
	"github.com/Tokutek/tokubenchmark"
	"github.com/Tokutek/tokubenchmark/mongotools"
	"labix.org/v2/mgo"
	"log"
	"math/rand"
	"time"
)

func main() {
	// needed for making/accessing collections:
	host := flag.String("host", "localhost", "host:port string of database to connect to")
	dbname := "partitionStress"
	collname := "partitionStress"

	flag.Parse()

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

	mongotools.MakeCollections(collname, dbname, 1, session, indexes)
	// at this point we have created the collection, now run the benchmark
	res := new(mongotools.IIBenchResult)
	numWriters := 8
	numQueryThreads := 16
	workers := make([]tokubenchmark.BenchmarkWorkInfo, 0, numWriters+numQueryThreads)
	currCollectionString := mongotools.GetCollectionString(collname, 0)
	for i := 0; i < numWriters; i++ {
		var gen *mongotools.IIBenchDocGenerator = new(mongotools.IIBenchDocGenerator)
		// we want each worker to have it's own random number generator
		// because generating random numbers takes a mutex
		gen.RandSource = rand.New(rand.NewSource(time.Now().UnixNano()))
		gen.CharFieldLength = 100
		gen.NumCharFields = 0
		workers = append(workers, mongotools.MakeCollectionWriter(gen, session, dbname, currCollectionString, 0))
	}
	for i := 0; i < numQueryThreads; i++ {
		copiedSession := session.Copy()
		copiedSession.SetSafe(&mgo.Safe{})
		query := mongotools.IIBenchQuery{
			copiedSession,
			dbname,
			currCollectionString,
			rand.New(rand.NewSource(time.Now().UnixNano())),
			time.Now(),
			100,
			0}
		workInfo := tokubenchmark.BenchmarkWorkInfo{query, 0, 0, 0}
		workers = append(workers, workInfo)
	}
	copiedSession := session.Copy()
	copiedSession.SetSafe(&mgo.Safe{})
	var addPartitionItem = mongotools.AddPartitionWorkItem{copiedSession, dbname, currCollectionString, 10 * time.Second}
	workers = append(workers, tokubenchmark.BenchmarkWorkInfo{addPartitionItem, 1, 1, 0})
	// have this go for a looooooong time
	tokubenchmark.RunBenchmark(res, workers, time.Duration(1<<32)*time.Second)
}
