package iibench

import (
	"flag"
	"fmt"
	"github.com/Tokutek/go-benchmark"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"math/rand"
	"time"
)

const (
	MaxNumCashRegisters = 1000
	MaxNumProducts      = 10000
	MaxNumCustomers     = 100000
	MaxPrice            = 500.0
)

var (
	// for QueryWork
	queryResultLimit   = flag.Int("queryResultLimit", 10, "number of results queries should be limited to")
	queriesPerInterval = flag.Uint64("queriesPerInterval", 100, "max queries per interval, 0 means unlimited")
	queryInterval      = flag.Uint64("queryInterval", 1, "interval for queries, in seconds, meant to be used with -queriesPerInterval")
	// for DocGenerator
	numCharFields   = flag.Int("numCharFields", 0, "specify the number of additional char fields stored in an array")
	charFieldLength = flag.Int("charFieldLength", 5, "specify length of char fields")
)

// what a document looks like
type doc struct {
	DateAndTime    time.Time "ts"
	CashRegisterID int32     "crid"
	CustomerID     int32     "cid"
	ProductID      int32     "pid"
	Price          float64   "pr"
	CharFields     []string  "cf,omitempty"
}

// information we use to generate an IIBench document
type DocGenerator struct {
	randSource      *rand.Rand
	NumCharFields   int
	CharFieldLength int
}

// this function inspired by http://devpy.wordpress.com/2013/10/24/create-random-string-in-golang/
func rand_str(str_size int, randSource *rand.Rand) string {
	alphanum := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, str_size)
	for i := 0; i < str_size; i++ {
		bytes[i] = alphanum[randSource.Int31n(int32(len(alphanum)))]
	}
	return string(bytes)
}

func NewDocGenerator() *DocGenerator {
	return &DocGenerator{randSource: rand.New(rand.NewSource(time.Now().UnixNano())), NumCharFields: *numCharFields, CharFieldLength: *charFieldLength}
}

// function to generate an iiBench document
func (g *DocGenerator) Generate() interface{} {
	dateAndTime := time.Now()
	cashRegisterID := g.randSource.Int31n(MaxNumCashRegisters)
	customerID := g.randSource.Int31n(MaxNumCustomers)
	productID := g.randSource.Int31n(MaxNumProducts)
	price := (g.randSource.Float64()*MaxPrice + float64(customerID)) / 100.0
	if g.NumCharFields > 0 {
		var charFields []string = make([]string, g.NumCharFields)
		for i := 0; i < len(charFields); i++ {
			charFields[i] = rand_str(g.CharFieldLength, g.randSource)
		}
		return doc{
			DateAndTime:    dateAndTime,
			CashRegisterID: cashRegisterID,
			CustomerID:     customerID,
			ProductID:      productID,
			Price:          price,
			CharFields:     charFields}
	}
	return doc{
		DateAndTime:    dateAndTime,
		CashRegisterID: cashRegisterID,
		CustomerID:     customerID,
		ProductID:      productID,
		Price:          price}
}

// a Work to run iibench queries
type QueryWork struct {
	session         *mgo.Session
	coll            *mgo.Collection
	randSource      *rand.Rand
	startTime       time.Time
	numQueriesSoFar uint64
}

func NewQueryWork(s *mgo.Session, db string, coll string) benchmark.WorkInfo {
	qw := &QueryWork{session: s, coll: s.DB(db).C(coll), randSource: rand.New(rand.NewSource(time.Now().UnixNano())), startTime: time.Now()}
	return benchmark.WorkInfo{qw, *queriesPerInterval, *queryInterval, 0}
}

func (qw *QueryWork) Do(c chan benchmark.Stats) {
	customerID := qw.randSource.Int31n(MaxNumCustomers)
	cashRegisterID := qw.randSource.Int31n(MaxNumCashRegisters)
	price := qw.randSource.Float64()*MaxPrice + float64(customerID)/100.0
	// generate a random time since qw.StartTime
	// there is likely a better way to do this
	since := time.Since(qw.startTime)
	sinceInNano := since.Nanoseconds()
	var randomTime int64
	if sinceInNano > 0 {
		randomTime = qw.randSource.Int63n(sinceInNano)
	}
	timeToQuery := qw.startTime.Add(time.Duration(randomTime))

	var query *mgo.Query
	if qw.numQueriesSoFar%3 == 0 {
		filter := bson.M{"$or": []bson.M{
			bson.M{"pr": price, "ts": timeToQuery, "cid": bson.M{"$gte": customerID}},
			bson.M{"pr": price, "ts": bson.M{"$gt": timeToQuery}},
			bson.M{"pr": bson.M{"$gt": price}}}}
		projection := bson.M{"pr": 1, "ts": 1, "cid": 1}
		query = qw.coll.Find(filter).Select(projection).Hint("pr", "ts", "cid")
	} else if qw.numQueriesSoFar%3 == 1 {
		filter := bson.M{"$or": []bson.M{
			bson.M{"crid": cashRegisterID, "pr": price, "cid": bson.M{"$gte": customerID}},
			bson.M{"crid": cashRegisterID, "pr": bson.M{"$gt": price}},
			bson.M{"crid": bson.M{"$gt": cashRegisterID}}}}
		projection := bson.M{"crid": 1, "pr": 1, "cid": 1}
		query = qw.coll.Find(filter).Select(projection).Hint("crid", "pr", "cid")
	} else {
		filter := bson.M{"$or": []bson.M{
			bson.M{"pr": price, "cid": bson.M{"$gte": customerID}},
			bson.M{"pr": bson.M{"$gt": price}}}}
		projection := bson.M{"pr": 1, "cid": 1}
		query = qw.coll.Find(filter).Select(projection).Hint("pr", "cid")
	}

	var result bson.M
	iter := query.Limit(*queryResultLimit).Iter()
	for iter.Next(&result) {
	}
	qw.numQueriesSoFar++
	c <- benchmark.Stats{Queries: 1}
}

func (qw *QueryWork) Close() {
	qw.session.Close()
}

// implements ResultManager
// used to print results of an iibench run
type IIBenchResult struct {
	NumInserts          uint64
	NumQueries          uint64
	LastInsertsReported uint64
	LastQueriesReported uint64
}

func (r *IIBenchResult) PrintResults() {
	lastInserts := r.NumInserts - r.LastInsertsReported
	lastQueries := r.NumQueries - r.LastQueriesReported
	fmt.Println("last insert", lastInserts, "total insert", r.NumInserts, "last queries", lastQueries, "total query", r.NumQueries)
	r.LastInsertsReported = r.NumInserts
	r.LastQueriesReported = r.NumQueries
}

func (r *IIBenchResult) PrintFinalResults() {
	fmt.Println("Benchmark done. Inserts: ", r.NumInserts, ", Queries: ", r.NumQueries)
}

func (r *IIBenchResult) RegisterIntermediateResult(result benchmark.Stats) {
	r.NumInserts += result.Inserts
	r.NumQueries += result.Queries
}
