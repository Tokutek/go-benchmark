package main

import (
	"flag"
	"fmt"
	"github.com/Tokutek/go-benchmark"
	"github.com/Tokutek/go-benchmark/mongotools"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"math/rand"
	"time"
)

//
//
// This benchmarks updates (or findAndModify) by _id fields on a sysbench collection
//
//

// implements Work
type SysbenchUpdateInfo struct {
	Session         *mgo.Session
	Dbname          string
	Collname        string
	RandSource      *rand.Rand
	NumCollections  int
	MaxID           int64
	doFindAndModify bool // if true, use findAndModify, else use updates
}

func runQuery(filter bson.M, projection bson.M, coll *mgo.Collection) {
	var result bson.M
	iter := coll.Find(filter).Select(projection).Iter()
	for iter.Next(&result) {
	}
}

func (s SysbenchUpdateInfo) Do(c chan<- interface{}) {
	db := s.Session.DB(s.Dbname)
	collectionIndex := s.RandSource.Int31n(int32(s.NumCollections))
	coll := db.C(mongotools.GetCollectionString(s.Collname, int(collectionIndex)))
	var sbresult SysbenchUpdateResult

	var i uint

	//db.sbtest8.update({_id: 5523412}, {$set: {c: "hello there"}}, false, false)
	// have each thread do 50 updates before sending results
	for i = 0; i < 50; i++ {
		randID := s.RandSource.Int63n(s.MaxID)
		var err error
		if s.doFindAndModify {
			change := mgo.Change{
				Update:    bson.M{"$inc": bson.M{"d": 1}},
				ReturnNew: true,
			}
			var info *mgo.ChangeInfo
			var doc bson.M
			info, err = coll.Find(bson.M{"_id": randID}).Apply(change, &doc)
			info = info // to make the compiler shut up about unused variable info.
		} else {
			err = coll.Update(bson.M{"_id": randID}, bson.M{"$inc": bson.M{"d": 1}})
		}
		if err != nil {
			// we got an error
			sbresult.NumErrors++
		}

		// send result over channel
		sbresult.NumUpdates++
	}
	c <- sbresult
}

func (s SysbenchUpdateInfo) Close() {
}

// implements ResultManager
type SysbenchUpdateResult struct {
	NumUpdates uint64 `type:"counter" report:"iter,cum,total"`
	NumErrors  uint64 `type:"counter" report:"total"`
}

var (
	// needed for making/accessing collections:
	host           = flag.String("host", "localhost", "host:port string of database to connect to")
	dbname         = flag.String("db", "sysbench", "dbname")
	collname       = flag.String("coll", "sbtest", "collname")
	numCollections = flag.Int("numCollections", 16, "number of collections")

	// for benchmark
	numThreads    = flag.Uint("numThreads", 64, "specify the number of threads")
	numMaxInserts = flag.Int64("numMaxInserts", 10000000, "number of documents in each collection")
	numSeconds    = flag.Uint64("numSeconds", 600, "number of seconds the benchmark is to run.")
	numMaxTPS     = flag.Uint64("numMaxTPS", 0, "number of maximum transactions to process. If 0, then unlimited")

	doFindAndModify = flag.Bool("findAndModify", false, "whether to use findAndModify instead of update")
)

func main() {
	flag.Parse()

	numTPSPerThread := (*numMaxTPS) / (uint64(*numThreads))

	session, err := mgo.Dial(*host)
	if err != nil {
		log.Fatal("Error connecting to ", *host, ": ", err)
	}
	// so we are not in fire and forget
	session.SetSafe(&mgo.Safe{})
	defer session.Close()

	mongotools.VerifyNotCreating()
	// just verifies that collections exist
	mongotools.MakeCollections(*collname, *dbname, *numCollections, session, make([]mgo.Index, 0))

	workers := make([]benchmark.WorkInfo, 0, *numThreads)
	var i uint
	for i = 0; i < *numThreads; i++ {
		copiedSession := session.Copy()
		defer copiedSession.Close()
		// allows transactions to be run on this session
		copiedSession.SetMode(mgo.Strong, true)
		var currItem benchmark.Work = SysbenchUpdateInfo{
			copiedSession,
			*dbname,
			*collname,
			rand.New(rand.NewSource(time.Now().UnixNano())),
			*numCollections,
			*numMaxInserts,
			*doFindAndModify}
		var currInfo benchmark.WorkInfo = benchmark.WorkInfo{currItem, numTPSPerThread, 1, 0}
		workers = append(workers, currInfo)
	}
	res := new(SysbenchUpdateResult)
	fmt.Println("passing in ", *numSeconds)
	benchmark.Run(res, workers, time.Duration(*numSeconds)*time.Second)
}
