package main

import (
	"flag"
	"github.com/zkasheff/tokubenchmark"
	"github.com/zkasheff/tokubenchmark/mongotools"
	"labix.org/v2/mgo"
	"log"
	"math/rand"
	"time"
)

type SysbenchDoc struct {
	Id  uint64 "_id"
	K   int    "k"
	C   string "c"
	Pad string "pad"
}

var ctemplate string = "###########-###########-###########-###########-###########-###########-###########-###########-###########-###########"
var padtemplate string = "###########-###########-###########-###########-###########"

func SysbenchString(template string, randSource *rand.Rand) string {
	var bytes = make([]byte, len(template))
	alpha := "abcdefghijklmnopqrstuvwxyz"
	nums := "0123456789"
	for i := 0; i < len(template); i++ {
		if template[i] == '#' {
			bytes[i] = nums[randSource.Int31n(int32(len(nums)))]
		} else if template[i] == '@' {
			bytes[i] += alpha[randSource.Int31n(int32(len(alpha)))]
		} else {
			bytes[i] += template[i]
		}
	}
	return string(bytes)
}

type SysbenchDocGenerator struct {
	RandSource *rand.Rand
	currID     uint64
}

func (generator *SysbenchDocGenerator) MakeDoc() interface{} {
	ret := SysbenchDoc{
		generator.currID,
		generator.RandSource.Int(),
		SysbenchString(ctemplate, generator.RandSource),
		SysbenchString(padtemplate, generator.RandSource)}
	generator.currID++
	return ret
}

// implements BenchmarkWorkItem
type SysbenchWriter struct {
	writers []tokubenchmark.BenchmarkWorkInfo
}

func (w SysbenchWriter) Close() {
	for x := range w.writers {
		w.writers[x].WorkItem.Close()
	}
}

func (w SysbenchWriter) DoWork(c chan tokubenchmark.BenchmarkStats) {
	for x := range w.writers {
		w.writers[x].WorkItem.DoWork(c)
	}
}

func main() {
	// needed for making/accessing collections:
	host := flag.String("host", "localhost", "host:port string of database to connect to")
	dbname := flag.String("db", "sysbench", "dbname")
	collname := flag.String("coll", "sbtest", "collname")
	numCollections := flag.Int("numCollections", 16, "number of collections to simultaneously run on")

	// for benchmark
	numWriters := flag.Int("numWriters", 16, "specify the number of writer threads")
	numInsertsPerCollection := flag.Uint64("numInsertsPerCollection", 100, "number of inserts to be done per collection")

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
	res := new(mongotools.IIBenchResult)
	workers := make([]tokubenchmark.BenchmarkWorkInfo, 0, *numWriters)

	var writers []SysbenchWriter = make([]SysbenchWriter, *numWriters)
	for i := 0; i < *numCollections; i++ {
		currCollectionString := mongotools.GetCollectionString(*collname, i)
		var gen *SysbenchDocGenerator = new(SysbenchDocGenerator)
		gen.RandSource = rand.New(rand.NewSource(time.Now().UnixNano()))
		var curr tokubenchmark.BenchmarkWorkInfo = mongotools.MakeCollectionWriter(gen, session, *dbname, currCollectionString, *numInsertsPerCollection)
		writers[i%*numWriters].writers = append(writers[i%*numWriters].writers, curr)
	}
	for i := 0; i < *numWriters; i++ {
		var curr tokubenchmark.BenchmarkWorkInfo = tokubenchmark.BenchmarkWorkInfo{WorkItem: writers[i]}
		curr.MaxOps = writers[i].writers[0].MaxOps
		workers = append(workers, curr)
	}
	tokubenchmark.RunBenchmark(res, workers, 0)
}
