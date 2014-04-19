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
	max        bson.M    `bson:"max"`
	CreateTime time.Time `bson:"createTime"`
}

type partitionInfo struct {
	NumPartitions uint64              `bson:"numPartitions"`
	Partitions    []indvPartitionInfo `bson:"partitions"`
	Ok            int                 `bson:"ok"`
}

// a BenchmarkWorkItem used to add partitions to a partitioned
// collection. To work, -partition=true must be used when creating
// the benchmark, otherwise, this workitem will spit errors.
type AddPartitionWorkItem struct {
	Session  *mgo.Session
	Dbname   string
	Collname string
	Interval time.Duration // interval between partition adds
}

// checks the createTime of the last partition, and if it happened
// longer than Interval defined in AddPartitionWorkItem, then it adds a partition.
// For example, if a.Interval is set to one hour, and the last partition was created
// 61 minutes ago, this function will add a partition
func (a AddPartitionWorkItem) DoWork(c chan tokubenchmark.BenchmarkStats) {
	db := a.Session.DB(a.Dbname)
	coll := db.C(a.Collname)
	var result partitionInfo
	err := db.Run(bson.M{"getPartitionInfo": coll.Name}, &result)
	if err == nil {
		var numPartitions uint64
		numPartitions = result.NumPartitions
		var lastCreateTime time.Time
		lastCreateTime = result.Partitions[numPartitions-1].CreateTime
		currentTime := time.Now()
		difference := currentTime.Sub(lastCreateTime)
		if difference > a.Interval {
			var addPartitionResult bson.M
			err = db.Run(bson.M{"addPartition": coll.Name}, &addPartitionResult)
			if err != nil {
				fmt.Println("error while adding Partition, ", err)
			}
		}
	} else {
		fmt.Println("error while getting Partition info, ", err)
	}
}

// closes the session used to add partitions
func (a AddPartitionWorkItem) Close() {
	a.Session.Close()
}

// a BenchmarkWorkItem used to drop partitions of a partitioned
// collection. To work, -partition=true must be used when creating
// the benchmark, otherwise, this workitem will spit errors.
type DropPartitionWorkItem struct {
	Session  *mgo.Session
	Dbname   string
	Collname string
	Interval time.Duration // interval between partition adds
}

// checks the createTime of the first partition, and if it happened
// longer than Interval defined in DropPartitionWorkItem, then it drops the first partition.
// For example, if a.Interval is set to six hours, and the first partition was created
// seven hours ago, this function will drop the first partition
func (a DropPartitionWorkItem) DoWork(c chan tokubenchmark.BenchmarkStats) {
	db := a.Session.DB(a.Dbname)
	coll := db.C(a.Collname)
	var result partitionInfo
	err := db.Run(bson.M{"getPartitionInfo": coll.Name}, &result)
	if err == nil {
		var numPartitions uint64
		numPartitions = result.NumPartitions
		var firstCreateTime time.Time
		firstCreateTime = result.Partitions[0].CreateTime
		currentTime := time.Now()
		difference := currentTime.Sub(firstCreateTime)
		if numPartitions > 0 && difference > a.Interval {
			firstID := result.Partitions[0].Id
			var dropPartitionResult bson.M
			err = db.Run(bson.D{{"dropPartition", coll.Name}, {"id", firstID}}, &dropPartitionResult)
			if err != nil {
				fmt.Println("error while dropping Partition, ", err)
			}
		}
	} else {
		fmt.Println("error while getting Partition info, ", err)
	}
}

func (a DropPartitionWorkItem) Close() {
	a.Session.Close()
}
