package mongotools

import (
	"flag"
	"fmt"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
)

// command line variables for creating collections
var doCreate *bool = flag.Bool("create", false, "whether the test should create the collection")
var nodeSize *int = flag.Int("nodeSize", 4*1024*1024, "specify the node size of all indexes in the collection, only takes affect if -create is true")
var basementSize *int = flag.Int("basementSize", 64*1024, "specify the basement node size of all indexes in the collection, only takes affect if -create is true")
var compression *string = flag.String("compression", zlib, "specify compression type of all indexes in the collection. Only takes affect if -create is true. Can be \"zlib\", \"lzma\", \"quicklz\", or \"none\"")
var partition *bool = flag.Bool("partition", false, "whether to partition the collections on create")

var IsTokuMX bool = false

//////////////////////////////////////////////////////////////////
//
//Code for creating the collection, be it in MongoDB or TokuMX
//
// used to run the create collection command

func GetCollectionString(collname string, i int) string {
	return fmt.Sprintf("%s_%d", collname, i)
}

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

// true if TokuMX, false if MongoDB
func setIsTokuMX(db *mgo.Database) {
	var result bson.M
	err := db.Run("buildInfo", &result)
	if err != nil {
		log.Fatal("Error when getting build info", err)
	}
	fmt.Println("here")
	if result["tokumxVersion"] == nil {
		log.Println("indexing technology: MongoDB")
		IsTokuMX = false
	}
	log.Println("indexing technology: TokuMX")
	IsTokuMX = true
}

func createMongoCollection(collname string, db *mgo.Database) {
	fmt.Println("creating collection: ", collname)
	var result bson.M
	createCmd := createCollOptions{Coll: collname}
	err := db.Run(createCmd, &result)
	if err != nil {
		log.Fatal(err)
	}
}

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

func createCollection(s *mgo.Session, dbname string, collname string, tokuOptions tokuMXCreateOptions, indexes []mgo.Index) {
	db := s.DB(dbname)
	coll := db.C(collname)
	if collectionExists(s, dbname, collname) {
		log.Fatal(coll.FullName, " exists, found in system.namespaces, run without -create")
	}
	if IsTokuMX {
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
}

func MakeCollections(collname string, dbname string, numCollections int, session *mgo.Session, indexes []mgo.Index) {
	if !validCompressionType(*compression) {
		log.Fatal("invalid value for compression: ", *compression)
	}
	db := session.DB(dbname)
	setIsTokuMX(db)
	for i := 0; i < numCollections; i++ {
		currCollectionString := GetCollectionString(collname, i)
		if *doCreate {
			createCollection(session, dbname, currCollectionString, tokuMXCreateOptions{*compression, *nodeSize, *basementSize, *partition}, indexes)
		} else if !collectionExists(session, dbname, currCollectionString) {
			log.Fatal("Collection ", dbname, ".", currCollectionString, " does not exist. Run with -create=true")
		}
	}
}

func VerifyNotCreating() {
	if *doCreate {
		log.Fatal("This application should not be creating collections, it should be using existing collections, -create must be false")
	}
}
