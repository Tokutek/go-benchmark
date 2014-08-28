package main

import (
	"flag"
	"fmt"
	"github.com/Tokutek/go-benchmark"
	"github.com/Tokutek/go-benchmark/benchmarks/sysbench"
	"github.com/Tokutek/go-benchmark/mongotools"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"math/rand"
	"time"
)

type SysbenchInfo struct {
	oltpRangeSize       uint
	oltpPointSelects    uint
	oltpSimpleRanges    uint
	oltpSumRanges       uint
	oltpOrderRanges     uint
	oltpDistinctRanges  uint
	oltpIndexUpdates    uint
	oltpNonIndexUpdates uint
}

// implements Work
type SysbenchTransaction struct {
	Info           SysbenchInfo
	Session        *mgo.Session
	Dbname         string
	Collname       string
	RandSource     *rand.Rand
	NumCollections int
	ReadOnly       bool
	MaxID          int64
}

func runQuery(filter bson.M, projection bson.M, coll *mgo.Collection) {
	var result bson.M
	iter := coll.Find(filter).Select(projection).Iter()
	for iter.Next(&result) {
	}
}

func (s SysbenchTransaction) Do(c chan<- interface{}) {
	db := s.Session.DB(s.Dbname)
	collectionIndex := s.RandSource.Int31n(int32(s.NumCollections))
	coll := db.C(mongotools.GetCollectionString(s.Collname, int(collectionIndex)))
	var sbresult SysbenchResult

	txn := mongotools.Transaction{DB: db}
	if err := txn.Begin(); err != nil {
		sbresult.NumErrors++
	}
	defer txn.Close()

	var i uint
	for i = 0; i < s.Info.oltpPointSelects; i++ {
		// db.sbtest8.find({_id: 554312}, {c: 1, _id: 0})
		filter := bson.M{"_id": s.RandSource.Int63n(int64(s.MaxID))}
		projection := bson.M{"c": 1}
		runQuery(filter, projection, coll)
	}
	for i = 0; i < s.Info.oltpSimpleRanges; i++ {
		//db.sbtest8.find({_id: {$gte: 5523412, $lte: 5523512}}, {c: 1, _id: 0})
		startID := s.RandSource.Int63n(s.MaxID)
		endID := startID + int64(s.Info.oltpRangeSize)
		filter := bson.M{"_id": bson.M{"$gte": startID, "$lt": endID}}
		projection := bson.M{"c": 1}
		runQuery(filter, projection, coll)
	}
	for i = 0; i < s.Info.oltpSumRanges; i++ {
		//db.sbtest8.aggregate([ {$match: {_id: {$gt: 5523412, $lt: 5523512}}}, { $group: { _id: null, total: { $sum: "$k"}} } ])
		startID := s.RandSource.Int63n(s.MaxID)
		endID := startID + int64(s.Info.oltpRangeSize)
		firstPipe := bson.M{"$match": bson.M{"_id": bson.M{"$gt": startID, "$lt": endID}}}
		secondPipe := bson.M{"$group": bson.M{"_id": nil, "total": bson.M{"$sum": "$k"}}} // is this $k correct?
		pipe := coll.Pipe([]bson.M{firstPipe, secondPipe})
		iter := pipe.Iter()
		var result bson.M
		for iter.Next(&result) {
		}
	}
	for i = 0; i < s.Info.oltpOrderRanges; i++ {
		//db.sbtest8.find({_id: {$gte: 5523412, $lte: 5523512}}, {c: 1, _id: 0}).sort({c: 1})
		startID := s.RandSource.Int63n(s.MaxID)
		endID := startID + int64(s.Info.oltpRangeSize)
		filter := bson.M{"_id": bson.M{"$gte": startID, "$lt": endID}}
		projection := bson.M{"c": 1}
		var result bson.M
		iter := coll.Find(filter).Select(projection).Sort("c").Iter()
		for iter.Next(&result) {
		}
	}
	for i = 0; i < s.Info.oltpDistinctRanges; i++ {
		//db.sbtest8.distinct("c",{_id: {$gt: 5523412, $lt: 5523512}}).sort()
		startID := s.RandSource.Int63n(s.MaxID)
		endID := startID + int64(s.Info.oltpRangeSize)
		filter := bson.M{"_id": bson.M{"$gte": startID, "$lt": endID}}
		var distinctResults []string
		err := coll.Find(filter).Distinct("c", &distinctResults)
		if err != nil {
			// we got an error
			sbresult.NumErrors++
		}
	}
	if !s.ReadOnly {
		for i = 0; i < s.Info.oltpIndexUpdates; i++ {
			//db.sbtest8.update({_id: 5523412}, {$inc: {k: 1}}, false, false)
			randID := s.RandSource.Int63n(s.MaxID)
			err := coll.Update(bson.M{"_id": randID}, bson.M{"$inc": bson.M{"k": 1}})
			if err != nil {
				// we got an error
				sbresult.NumErrors++
			}
		}
		for i = 0; i < s.Info.oltpNonIndexUpdates; i++ {
			//db.sbtest8.update({_id: 5523412}, {$set: {c: "hello there"}}, false, false)
			randID := s.RandSource.Int63n(s.MaxID)
			err := coll.Update(bson.M{"_id": randID}, bson.M{"$set": bson.M{"c": sysbench.CString(s.RandSource)}})
			if err != nil {
				// we got an error
				sbresult.NumErrors++
			}
		}
	}
	// remove an ID
	// re-insert the ID
	randID := s.RandSource.Int63n(s.MaxID)
	err := coll.Remove(bson.M{"_id": randID})
	if err != nil {
		// we got an error
		sbresult.NumErrors++
	}
	// TODO: re-insert the ID
	err = coll.Insert(sysbench.Doc{
		uint64(randID),
		s.RandSource.Int(),
		s.RandSource.Int(),
		sysbench.CString(s.RandSource),
		sysbench.PadString(s.RandSource)})
	if err != nil {
		// we got an error
		sbresult.NumErrors++
	} else {
		txn.Commit()
	}

	// send result over channel
	sbresult.NumTransactions++
	c <- sbresult
}

func (s SysbenchTransaction) Close() {
}

// implements ResultManager
type SysbenchResult struct {
	NumTransactions uint64 `type:"counter" report:"iter,cum,total"`
	NumErrors       uint64 `type:"counter" report:"total"`
}

var (
	// needed for making/accessing collections:
	host           = flag.String("host", "localhost", "host:port string of database to connect to")
	dbname         = flag.String("db", "sysbench", "dbname")
	collname       = flag.String("coll", "sbtest", "collname")
	numCollections = flag.Int("numCollections", 16, "number of collections")
	readOnly       = flag.Bool("readOnly", false, "if true, then updates excluded from benchmark")

	// for benchmark
	numThreads    = flag.Uint("numThreads", 64, "specify the number of threads")
	numMaxInserts = flag.Int64("numMaxInserts", 10000000, "number of documents in each collection")
	numSeconds    = flag.Uint64("numSeconds", 600, "number of seconds the benchmark is to run.")
	numMaxTPS     = flag.Uint64("numMaxTPS", 0, "number of maximum transactions to process. If 0, then unlimited")

	// for the Work
	oltpRangeSize       = flag.Uint("oltpRangeSize", 100, "size of range queries in each transaction")
	oltpPointSelects    = flag.Uint("oltpPointSelects", 10, "number of point queries by _id per transaction")
	oltpSimpleRanges    = flag.Uint("oltpSimpleRanges", 1, "number of simple range queries per transaction")
	oltpSumRanges       = flag.Uint("oltpSumRanges", 1, "number of aggregation queries that sum a field per transaction")
	oltpOrderRanges     = flag.Uint("oltpOrderRanges", 1, "number of range queries sorted on a field per transaction")
	oltpDistinctRanges  = flag.Uint("oltpDistinctRanges", 1, "number of aggregation queries using disting per transaction ")
	oltpIndexUpdates    = flag.Uint("oltpIndexUpdates", 1, "number of updates on an indexed field per transaction")
	oltpNonIndexUpdates = flag.Uint("oltpNonIndexUpdates", 1, "number of updates on a non-indexed field per transaction")
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

	info := SysbenchInfo{
		*oltpRangeSize,
		*oltpPointSelects,
		*oltpSimpleRanges,
		*oltpSumRanges,
		*oltpOrderRanges,
		*oltpDistinctRanges,
		*oltpIndexUpdates,
		*oltpNonIndexUpdates}
	workers := make([]benchmark.WorkInfo, 0, *numThreads)
	var i uint
	for i = 0; i < *numThreads; i++ {
		copiedSession := session.Copy()
		defer copiedSession.Close()
		// allows transactions to be run on this session
		copiedSession.SetMode(mgo.Strong, true)
		var currItem benchmark.Work = SysbenchTransaction{
			info,
			copiedSession,
			*dbname,
			*collname,
			rand.New(rand.NewSource(time.Now().UnixNano())),
			*numCollections,
			*readOnly,
			*numMaxInserts}
		var currInfo benchmark.WorkInfo = benchmark.WorkInfo{currItem, numTPSPerThread, 1, 0}
		workers = append(workers, currInfo)
	}
	res := new(SysbenchResult)
	fmt.Println("passing in ", *numSeconds)
	benchmark.Run(res, workers, time.Duration(*numSeconds)*time.Second)
}
