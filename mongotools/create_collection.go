package mongotools

import (
	"flag"
	"fmt"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
)

// command line variables for creating collections
var (
	doCreate     = flag.Bool("create", false, "whether the test should create the collection")
	nodeSize     = flag.Int("nodeSize", 4*1024*1024, "specify the node size of all indexes in the collection, only takes affect if -create is true")
	basementSize = flag.Int("basementSize", 64*1024, "specify the basement node size of all indexes in the collection, only takes affect if -create is true")
	compression  = flag.String("compression", zlib, "specify compression type of all indexes in the collection. Only takes affect if -create is true. Can be \"zlib\", \"lzma\", \"quicklz\", or \"none\"")
	partition    = flag.Bool("partition", false, "whether to partition the collections on create")
)

//////////////////////////////////////////////////////////////////
//
//Code for creating the collection, be it in MongoDB or TokuMX
//
// used to run the create collection command

// return the name for the ith collection (indexed from 0) that was created
// via MakeCollections
func GetCollectionString(collname string, i int) string {
	return fmt.Sprintf("%s_%d", collname, i)
}

// options for creating a collection
// TODO: add an option for primary key
type createCollOptions struct {
	Coll         string "create"
	Compression  string "compression,omitempty"
	NodeSize     int    "pageSize,omitempty"
	BasementSize int    "readPageSize,omitempty"
	Partitioned  bool   "partitioned,omitempty"
}

const (
	zlib           = "zlib"
	quicklz        = "quicklz"
	lzma           = "lzma"
	no_compression = "none"
)

func validCompressionType(compressionType string) bool {
	return (compressionType == zlib || compressionType == quicklz || compressionType == lzma) || (compressionType == no_compression)
}

type tokuMXCreateOptions struct {
	CompressionType string
	NodeSize        int
	BasementSize    int
	Partitioned     bool
}

func getDefaultCreateOptions() (ret tokuMXCreateOptions) {
	ret.CompressionType = zlib
	ret.NodeSize = 1 << 22     //4MB
	ret.BasementSize = 1 << 16 //64KB
	return ret
}

// creates a collection in MongoDB
func createMongoCollection(collname string, db *mgo.Database) {
	fmt.Println("creating collection: ", collname)
	var result bson.M
	createCmd := createCollOptions{Coll: collname}
	err := db.Run(createCmd, &result)
	if err != nil {
		log.Fatal(err)
	}
}

// creates a collection in TokuMX
func createTokuCollection(collname string, db *mgo.Database, options tokuMXCreateOptions) {
	fmt.Println("creating collection ", collname)
	var result bson.M
	createCmd := createCollOptions{
		Coll:         collname,
		Compression:  options.CompressionType,
		NodeSize:     options.NodeSize,
		BasementSize: options.BasementSize,
		Partitioned:  options.Partitioned}
	err := db.Run(createCmd, &result)
	if err != nil {
		log.Fatal(err)
	}
}

// returns true if the collection already exists. Does so
// by querying db.system.namespaces to see if the collection
// is listed
func collectionExists(s *mgo.Session, dbname string, collname string) bool {
	db := s.DB(dbname)
	sysNamespaces := db.C("system.namespaces")
	coll := db.C(collname)
	q := sysNamespaces.Find(bson.M{"name": coll.FullName})
	n, err := q.Count()
	if err != nil {
		log.Fatal("Received error ", err, " when querying system.namespaces, exiting")
	}
	return n > 0
}

func applyTokuIndexOptions(db *mgo.Database, collname string, options tokuMXCreateOptions) {
	var result bson.M
	var optBson bson.M
	optBson = bson.M{"compression": options.CompressionType , "pageSize" : options.NodeSize, "readPageSize" : options.BasementSize}
	err := db.Run(bson.D{{"reIndex", collname}, {"index", "*"}, {"options" , optBson}}, &result)
	if err != nil {
		log.Fatal("Failed to set options on indexes, received ", err)
	}
}

// Creates a collection, but first checks if the collection exists. If it does,
// we log a fatal error and end the program
func createCollection(s *mgo.Session, dbname string, collname string, tokuOptions tokuMXCreateOptions, indexes []mgo.Index) {
	db := s.DB(dbname)
	coll := db.C(collname)
	if collectionExists(s, dbname, collname) {
		log.Fatal(coll.FullName, " exists, found in system.namespaces, run without -create")
	}
	if IsTokuMX(db) {
		createTokuCollection(collname, db, tokuOptions)
	} else {
		createMongoCollection(collname, db)
	}
	for x := range indexes {
		err := coll.EnsureIndex(indexes[x])
		if err != nil {
			log.Fatal("Received error ", err, " when adding index ", indexes[x])
		}
	}
	if IsTokuMX(db) {
		applyTokuIndexOptions(db, collname, tokuOptions)
	}
}

// Either creates or ensures the existence of the collections to be used in the benchmark. If the create
// flag is set to true (which is defined in this file), then this function creates the collections specified
// as follows. If collname is "coll", and dbname is "dbb", and numCollections is 2, then the collections
// "dbb.coll_0" and "dbb.coll_1" are created. Additionally, if we are creating these collections,
// then the indexes specified in the last parameter are created.
//
// If either of these collections already exist, then a fatal
// error is logged and the program ends. If the create flag is set to false, then this function
// ensures that the specified collections ("dbb.coll_0" and "dbb.coll_1" in the example) already exist.
// We do NOT verify that the indexes passed into this function match the existing indexes of the collection
func MakeCollections(collname string, dbname string, numCollections int, session *mgo.Session, indexes []mgo.Index) {
	if !validCompressionType(*compression) {
		log.Fatal("invalid value for compression: ", *compression)
	}
	for i := 0; i < numCollections; i++ {
		currCollectionString := GetCollectionString(collname, i)
		if *doCreate {
			createCollection(session, dbname, currCollectionString, tokuMXCreateOptions{*compression, *nodeSize, *basementSize, *partition}, indexes)
		} else if !collectionExists(session, dbname, currCollectionString) {
			log.Fatal("Collection ", dbname, ".", currCollectionString, " does not exist. Run with -create=true")
		}
	}
}

// Ensures that the benchmark was started (and MakeCollections will be called) with the assumption
// that the collections are not to be created. Currently used in sysbench, where we assume the benchmark
// is run on preloaded collections.
func VerifyNotCreating() {
	if *doCreate {
		log.Fatal("This application should not be creating collections, it should be using existing collections, -create must be false")
	}
}
