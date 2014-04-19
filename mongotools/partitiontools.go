package mongotools

import (
	"fmt"
	"github.com/Tokutek/tokubenchmark"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"time"
)

type indvPartitionInfo struct {
	Id         uint64    `bson:"_id"`
	CreateTime time.Time `bson:"createTime"`
}

type partitionInfo struct {
	NumPartitions uint64              `bson:"numPartitions"`
	Partitions    []indvPartitionInfo `bson:"partitions"`
	Ok            int                 `bson:"ok"`
}

// implements BenchmarkWorkItem
type AddPartitionWorkItem struct {
	Session  *mgo.Session
	Dbname   string
	Collname string
	Interval time.Duration // interval between partition adds
}

func (a AddPartitionWorkItem) DoWork(c chan tokubenchmark.BenchmarkStats) {
	db := a.Session.DB(a.Dbname)
	coll := db.C(a.Collname)
	var result partitionInfo
	err := db.Run(bson.M{"getPartitionInfo": coll.Name}, &result)
	if err == nil {
		var numPartitions uint64
		numPartitions = result.NumPartitions
		fmt.Println("num partitions: ", numPartitions)
		var lastCreateTime time.Time
		lastCreateTime = result.Partitions[numPartitions-1].CreateTime
		fmt.Println("last create time", lastCreateTime)
	}
}

func (a AddPartitionWorkItem) Close() {
	a.Session.Close()
}
