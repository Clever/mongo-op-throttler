package apply

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/Clever/mongo-op-throttler/operation"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func setupDb(t *testing.T) *mgo.Database {
	session, err := mgo.Dial("localhost")
	assert.NoError(t, err)
	db := session.DB("throttle")
	assert.NoError(t, db.DropDatabase())
	return db
}

func createInsert(t *testing.T) []byte {
	doc := bson.M{
		"v":  2,
		"op": "i",
		"ns": "throttle.test",
		"o": bson.M{
			"_id": bson.NewObjectId(),
			"val": bson.NewObjectId(),
		},
	}
	bytes, err := bson.Marshal(doc)
	assert.NoError(t, err)
	return bytes
}

func TestApplySpeed(t *testing.T) {
	db := setupDb(t)

	buffer := bytes.NewBufferString("")
	for i := 0; i < 10; i++ {
		buffer.Write(createInsert(t))
	}

	start := time.Now()
	assert.NoError(t, ApplyOps(buffer, 5, db.Session))
	end := time.Now()
	millisElapsed := end.Sub(start).Nanoseconds() / (1000 * 1000)
	if millisElapsed < 1800 || millisElapsed > 2200 {
		assert.Fail(t, fmt.Sprintf("Duration outside expected range %d", millisElapsed))
	}

	count, err := db.C("test").Count()
	assert.NoError(t, err)
	assert.Equal(t, 10, count)
}

func TestApplyOps(t *testing.T) {
	// Test the full flow, end-to-end
	db := setupDb(t)

	toRemoveId := bson.NewObjectId()
	toUpdateId := bson.NewObjectId()
	toKeepTheSameId := bson.NewObjectId()
	toInsertId := bson.NewObjectId()

	assert.NoError(t, db.C("test").Insert(bson.M{"_id": toRemoveId, "key": "remove"}))
	assert.NoError(t, db.C("test").Insert(bson.M{"_id": toUpdateId, "key": "update"}))
	assert.NoError(t, db.C("test").Insert(bson.M{"_id": toKeepTheSameId, "key": "same"}))

	buffer := bytes.NewBufferString("")
	insertOplog := bson.M{
		"v":  2,
		"op": "i",
		"ns": "throttle.test",
		"o": bson.M{
			"_id": toInsertId,
			"key": "insert",
		},
	}
	bytes, err := bson.Marshal(insertOplog)
	assert.NoError(t, err)
	buffer.Write(bytes)

	updateOplog := bson.M{
		"v":  2,
		"op": "u",
		"ns": "throttle.test",
		"o": bson.M{
			"$set": bson.M{"key": "update2"},
		},
		"o2": bson.M{
			"_id": toUpdateId,
		},
	}
	bytes, err = bson.Marshal(updateOplog)
	assert.NoError(t, err)
	buffer.Write(bytes)

	removeDoc := bson.M{
		"v":  2,
		"op": "d",
		"ns": "throttle.test",
		"o": bson.M{
			"_id": toRemoveId,
		},
		"b": true,
	}
	bytes, err = bson.Marshal(removeDoc)
	assert.NoError(t, err)
	buffer.Write(bytes)

	assert.NoError(t, ApplyOps(buffer, 1000, db.Session))

	// Confirm that we have the expected number of docs
	count, err := db.C("test").Count()
	assert.NoError(t, err)
	assert.Equal(t, 3, count)

	// Check that each individual doc is there
	var doc bson.M
	assert.NoError(t, db.C("test").Find(bson.M{"_id": toInsertId}).One(&doc))
	assert.Equal(t, "insert", doc["key"])

	assert.NoError(t, db.C("test").Find(bson.M{"_id": toUpdateId}).One(&doc))
	assert.Equal(t, "update2", doc["key"])

	assert.NoError(t, db.C("test").Find(bson.M{"_id": toKeepTheSameId}).One(&doc))
	assert.Equal(t, "same", doc["key"])
}

func TestMissingNamespace(t *testing.T) {
	op := operation.Op{
		ID:        bson.NewObjectId().Hex(),
		Type:      "remove",
		Namespace: "bad",
	}
	err := applyOp(op, nil)
	assert.Error(t, err)
	assert.Equal(t, "Invalid namespace: bad", err.Error())
}

func TestInvalidObjectId(t *testing.T) {
	op := operation.Op{
		ID:        "bad",
		Type:      "insert",
		Namespace: "throttle.test",
	}
	err := applyOp(op, nil)
	assert.Error(t, err)
	assert.Equal(t, "Invalid ID: bad", err.Error())
}

func TestInvalidType(t *testing.T) {
	op := operation.Op{
		ID:        bson.NewObjectId().Hex(),
		Type:      "badop",
		Namespace: "throttle.test",
	}
	err := applyOp(op, nil)
	assert.Error(t, err)
	assert.Equal(t, "Unknown type: badop", err.Error())
}

func TestUpdate(t *testing.T) {
	db := setupDb(t)

	// Update a doc and check that it's changed
	obj := bson.M{"_id": bson.NewObjectId(), "key": "value"}
	assert.NoError(t, db.C("test").Insert(obj))

	updatedObj := bson.M{"key": "value2"}

	op := operation.Op{
		ID:        obj["_id"].(bson.ObjectId).Hex(),
		Type:      "update",
		Namespace: "throttle.test",
		Obj:       updatedObj,
	}
	assert.NoError(t, applyOp(op, db.Session))

	var result bson.M
	assert.NoError(t, db.C("test").Find(bson.M{}).One(&result))
	assert.Equal(t, "value2", result["key"].(string))

	// Try with the $set syntax
	op.Obj = bson.M{"$set": bson.M{"key": "value3"}}

	assert.NoError(t, applyOp(op, db.Session))
	assert.NoError(t, db.C("test").Find(bson.M{}).One(&result))
	assert.Equal(t, "value3", result["key"].(string))

	// Updating a doc that doesn't exist doesn't fail
	op.ID = bson.NewObjectId().Hex()
	assert.NoError(t, applyOp(op, db.Session))
}

func TestInsert(t *testing.T) {
	db := setupDb(t)

	id := bson.NewObjectId()
	obj := bson.M{"_id": id, "key": "value"}

	op := operation.Op{
		ID:        id.Hex(),
		Type:      "insert",
		Namespace: "throttle.test",
		Obj:       obj,
	}
	assert.NoError(t, applyOp(op, db.Session))

	var result bson.M
	assert.NoError(t, db.C("test").Find(bson.M{"_id": id}).One(&result))
	assert.Equal(t, "value", result["key"].(string))
}

func TestRemove(t *testing.T) {
	db := setupDb(t)

	id := bson.NewObjectId()
	assert.NoError(t, db.C("test").Insert(bson.M{"_id": id, "key": "value"}))

	op := operation.Op{
		ID:        id.Hex(),
		Type:      "remove",
		Namespace: "throttle.test",
	}
	assert.NoError(t, applyOp(op, db.Session))

	count, err := db.C("test").Count()
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}
