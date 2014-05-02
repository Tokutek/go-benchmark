package main

import (
	"flag"
	"github.com/Tokutek/go-benchmark"
	"github.com/Tokutek/go-benchmark/benchmarks/iibench"
	"github.com/Tokutek/go-benchmark/benchmarks/partition_stress"
	"github.com/Tokutek/go-benchmark/mongotools"
	"labix.org/v2/mgo"
	"log"
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
	res := new(iibench.IIBenchResult)
	numWriters := 8
	numQueryThreads := 16
	workers := make([]benchmark.WorkInfo, 0, numWriters+numQueryThreads)
	currCollectionString := mongotools.GetCollectionString(collname, 0)
	for i := 0; i < numWriters; i++ {
		var gen = iibench.NewDocGenerator()
		gen.CharFieldLength = 100
		gen.NumCharFields = 0
		workers = append(workers, mongotools.MakeCollectionWriter(gen, session, dbname, currCollectionString, 0))
	}
	for i := 0; i < numQueryThreads; i++ {
		copiedSession := session.Copy()
		copiedSession.SetSafe(&mgo.Safe{})
		workers = append(workers, iibench.NewQueryWork(copiedSession, dbname, currCollectionString))
	}
	{
		copiedSession := session.Copy()
		copiedSession.SetSafe(&mgo.Safe{})
		var addPartitionItem = partition_stress.AddPartitionWork{copiedSession, dbname, currCollectionString, time.Hour}
		workers = append(workers, benchmark.WorkInfo{addPartitionItem, 1, 1, 0})
	}
	{
		copiedSession := session.Copy()
		copiedSession.SetSafe(&mgo.Safe{})
		var dropPartitionItem = partition_stress.DropPartitionWork{copiedSession, dbname, currCollectionString, 7 * time.Hour}
		workers = append(workers, benchmark.WorkInfo{dropPartitionItem, 1, 1, 0})
	}
	// have this go for a looooooong time
	benchmark.Run(res, workers, time.Duration(1<<32)*time.Second)
}
