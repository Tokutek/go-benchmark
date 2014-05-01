package mongotools

import (
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
)

func IsTokuMX(db *mgo.Database) bool {
	var result bson.M
	if err := db.Run("buildInfo", &result); err != nil {
		log.Fatal(err)
	}
	return result["tokumxVersion"] != nil
}
