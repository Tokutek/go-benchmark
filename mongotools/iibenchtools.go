package mongotools

import (
	"fmt"
	"github.com/Tokutek/tokubenchmark"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"math/rand"
	"time"
)

//1000, 10000, 100000, 500
const (
	MaxNumCashRegisters = 1000
	MaxNumProducts      = 10000
	MaxNumCustomers     = 100000
	MaxPrice            = 500.0
)

////////////////////////////////////////////////////////////////
//
// Code to generate an IIBench document
//
//

// what a document looks like
type iiBenchDoc struct {
	DateAndTime    time.Time "ts"
	CashRegisterID int32     "crid"
	CustomerID     int32     "cid"
	ProductID      int32     "pid"
	Price          float64   "pr"
	CharFields     []string  "cf,omitempty"
}

// information we use to generate an IIBench document
type IIBenchDocGenerator struct {
	RandSource      *rand.Rand
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

// function to generate an iiBench document
func (generator *IIBenchDocGenerator) MakeDoc() interface{} {
	dateAndTime := time.Now()
	cashRegisterID := generator.RandSource.Int31n(MaxNumCashRegisters)
	customerID := generator.RandSource.Int31n(MaxNumCustomers)
	productID := generator.RandSource.Int31n(MaxNumProducts)
	price := (generator.RandSource.Float64()*MaxPrice + float64(customerID)) / 100.0
	if generator.NumCharFields > 0 {
		var charFields []string = make([]string, generator.NumCharFields)
		for i := 0; i < len(charFields); i++ {
			charFields[i] = rand_str(generator.CharFieldLength, generator.RandSource)
		}
		return iiBenchDoc{
			DateAndTime:    dateAndTime,
			CashRegisterID: cashRegisterID,
			CustomerID:     customerID,
			ProductID:      productID,
			Price:          price,
			CharFields:     charFields}
	}
	return iiBenchDoc{
		DateAndTime:    dateAndTime,
		CashRegisterID: cashRegisterID,
		CustomerID:     customerID,
		ProductID:      productID,
		Price:          price}
}

// a BenchmarkWorkItem to run iibench queries
type IIBenchQuery struct {
	Session          *mgo.Session
	Dbname           string
	Collname         string
	RandSource       *rand.Rand
	StartTime        time.Time
	QueryResultLimit int
	NumQueriesSoFar  uint64
}

func (r IIBenchQuery) DoWork(c chan tokubenchmark.BenchmarkStats) {
	db := r.Session.DB(r.Dbname)
	coll := db.C(r.Collname)
	customerID := r.RandSource.Int31n(MaxNumCustomers)
	cashRegisterID := r.RandSource.Int31n(MaxNumCashRegisters)
	price := r.RandSource.Float64()*MaxPrice + float64(customerID)/100.0
	// generate a random time since r.StartTime
	// there is likely a better way to do this
	since := time.Since(r.StartTime)
	sinceInNano := since.Nanoseconds()
	var randomTime int64
	if sinceInNano > 0 {
		randomTime = r.RandSource.Int63n(sinceInNano)
	}
	timeToQuery := r.StartTime.Add(time.Duration(randomTime))

	var query *mgo.Query
	if r.NumQueriesSoFar%3 == 0 {
		filter := bson.M{"$or": []bson.M{
			bson.M{"pr": price, "ts": timeToQuery, "cid": bson.M{"$gte": customerID}},
			bson.M{"pr": price, "ts": bson.M{"$gt": timeToQuery}},
			bson.M{"pr": bson.M{"$gt": price}}}}
		projection := bson.M{"pr": 1, "ts": 1, "cid": 1}
		query = coll.Find(filter).Select(projection).Hint("pr", "ts", "cid")
	} else if r.NumQueriesSoFar%3 == 1 {
		filter := bson.M{"$or": []bson.M{
			bson.M{"crid": cashRegisterID, "pr": price, "cid": bson.M{"$gte": customerID}},
			bson.M{"crid": cashRegisterID, "pr": bson.M{"$gt": price}},
			bson.M{"crid": bson.M{"$gt": cashRegisterID}}}}
		projection := bson.M{"crid": 1, "pr": 1, "cid": 1}
		query = coll.Find(filter).Select(projection).Hint("crid", "pr", "cid")
	} else {
		filter := bson.M{"$or": []bson.M{
			bson.M{"pr": price, "cid": bson.M{"$gte": customerID}},
			bson.M{"pr": bson.M{"$gt": price}}}}
		projection := bson.M{"pr": 1, "cid": 1}
		query = coll.Find(filter).Select(projection).Hint("pr", "cid")
	}

	var result bson.M
	iter := query.Limit(r.QueryResultLimit).Iter()
	for iter.Next(&result) {
	}
	r.NumQueriesSoFar++
	c <- tokubenchmark.BenchmarkStats{Queries: 1}
}

func (r IIBenchQuery) Close() {
	r.Session.Close()
}

// implements BenchmarkResultManager
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

func (r *IIBenchResult) RegisterIntermedieteResult(result tokubenchmark.BenchmarkStats) {
	r.NumInserts += result.Inserts
	r.NumQueries += result.Queries
}
