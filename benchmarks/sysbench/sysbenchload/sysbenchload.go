package main

import (
	"flag"
	"github.com/Tokutek/go-benchmark"
	"github.com/Tokutek/go-benchmark/benchmarks/iibench"
	"github.com/Tokutek/go-benchmark/benchmarks/sysbench"
	"github.com/Tokutek/go-benchmark/mongotools"
	"labix.org/v2/mgo"
	"log"
	"math/rand"
	"time"
)

type SysbenchDocGenerator struct {
	RandSource *rand.Rand
	currID     uint64
}

func (generator *SysbenchDocGenerator) Generate() interface{} {
	ret := sysbench.Doc{
		generator.currID,
		generator.RandSource.Int(),
		sysbench.CString(generator.RandSource),
		sysbench.PadString(generator.RandSource)}
	generator.currID++
	return ret
}

// implements Work
type SysbenchWriter struct {
	writers []benchmark.WorkInfo
}

func (w SysbenchWriter) Close() {
	for x := range w.writers {
		w.writers[x].Work.Close()
	}
}

func (w SysbenchWriter) Do(c chan<- interface{}) {
	for x := range w.writers {
		w.writers[x].Work.Do(c)
	}
}

var (
	// needed for making/accessing collections:
	host           = flag.String("host", "localhost", "host:port string of database to connect to")
	dbname         = flag.String("db", "sysbench", "dbname")
	collname       = flag.String("coll", "sbtest", "collname")
	numCollections = flag.Int("numCollections", 16, "number of collections to simultaneously run on")

	// for benchmark
	numWriters              = flag.Int("numWriters", 8, "specify the number of writer threads")
	numInsertsPerCollection = flag.Int("numInsertsPerCollection", 10000000, "number of inserts to be done per collection")
)

func main() {
	flag.Parse()
	if *numWriters > *numCollections {
		log.Fatal("numWriters should not be greater than numCollections")
	}

	session, err := mgo.Dial(*host)
	if err != nil {
		log.Fatal("Error connecting to ", *host, ": ", err)
	}
	// so we are not in fire and forget
	session.SetSafe(&mgo.Safe{})
	defer session.Close()

	indexes := make([]mgo.Index, 1)
	indexes[0] = mgo.Index{Key: []string{"k"}}

	mongotools.MakeCollections(*collname, *dbname, *numCollections, session, indexes)
	// at this point we have created the collection, now run the benchmark
	res := new(iibench.Result)
	workers := make([]benchmark.WorkInfo, 0, *numWriters)

	var writers []SysbenchWriter = make([]SysbenchWriter, *numWriters)
	for i := 0; i < *numCollections; i++ {
		copiedSession := session.Copy()
		defer copiedSession.Close()
		currCollectionString := mongotools.GetCollectionString(*collname, i)
		var gen *SysbenchDocGenerator = new(SysbenchDocGenerator)
		gen.RandSource = rand.New(rand.NewSource(time.Now().UnixNano()))
		var curr benchmark.WorkInfo = mongotools.NewInsertWork(gen, copiedSession.DB(*dbname).C(currCollectionString), *numInsertsPerCollection)
		writers[i%*numWriters].writers = append(writers[i%*numWriters].writers, curr)
	}
	for i := 0; i < *numWriters; i++ {
		var curr benchmark.WorkInfo = benchmark.WorkInfo{Work: writers[i]}
		curr.MaxOps = writers[i].writers[0].MaxOps
		workers = append(workers, curr)
	}
	benchmark.Run(res, workers, 0)
}
