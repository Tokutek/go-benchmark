package main

import (
	"flag"
	"fmt"
	"github.com/Tokutek/tokubenchmark/mongotools"
	"labix.org/v2/mgo"
	"log"
)

func main() {
	host := flag.String("host", "localhost", "host:port string of database to connect to")
	dbname := flag.String("db", "iibench", "dbname")
	collname := flag.String("coll", "purchases_index", "collname")
	numCollections := flag.Int("numCollections", 100, "number of collections to create per db")
	numDBs := flag.Int("numDBs", 100, "number of DBs to create")

	// will hard code 3 indexes.
	flag.Parse()
	session, err := mgo.Dial(*host)
	if err != nil {
		log.Fatal("Error connecting to ", *host, ": ", err)
	}
	// so we are not in fire and forget
	session.SetSafe(&mgo.Safe{})
	defer session.Close()

	// these are dummy indexes. We are not inserting data
	// we just want to create files
	indexes := make([]mgo.Index, 3)
	indexes[0] = mgo.Index{Key: []string{"pr", "cid"}}
	indexes[1] = mgo.Index{Key: []string{"crid", "pr", "cid"}}
	indexes[2] = mgo.Index{Key: []string{"pr", "ts", "cid"}}

	for i := 0; i < *numDBs; i++ {
		currDB := fmt.Sprintf("%s_%d", *dbname, i)
		mongotools.MakeCollections(*collname, currDB, *numCollections, session, indexes)
	}
}
