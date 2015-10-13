package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

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

	doc := bson.M{"key": "value"}
	bytes, err := bson.Marshal(doc)
	assert.NoError(t, err)

	encoded := base64.StdEncoding.EncodeToString(bytes)

	op := operation{
		ID:          bson.NewObjectId().Hex(),
		Type:        "insert",
		Namespace:   "throttle.test",
		EncodedBson: encoded,
	}

	bytes, err = json.Marshal(op)
	assert.NoError(t, err)
	return bytes
}

func TestApplySpeed(t *testing.T) {
	db := setupDb(t)

	buffer := bytes.NewBufferString("")
	for i := 0; i < 10; i++ {
		buffer.Write(createInsert(t))
		buffer.WriteRune('\n')
	}

	start := time.Now()
	assert.NoError(t, Apply(buffer, 5, "localhost"))
	end := time.Now()
	millisElapsed := end.Sub(start).Nanoseconds() / (1000 * 1000)
	if millisElapsed < 1800 || millisElapsed > 2200 {
		assert.Fail(t, fmt.Sprintf("Duration outside expected range %d", millisElapsed))
	}

	count, err := db.C("test").Count()
	assert.NoError(t, err)
	assert.Equal(t, 10, count)
}

func TestInvalidJson(t *testing.T) {
	buffer := bytes.NewBufferString("badJson")
	err := Apply(buffer, 5, "localhost")
	assert.Error(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "Error parsing json"))
}

func TestMissingNamespace(t *testing.T) {
	op := operation{
		ID:          bson.NewObjectId().Hex(),
		Type:        "remove",
		Namespace:   "bad",
		EncodedBson: "",
	}
	err := applyOp(op, nil)
	assert.Error(t, err)
	assert.Equal(t, "Invalid namespace: bad", err.Error())
}

func TestInvalidObjectId(t *testing.T) {
	op := operation{
		ID:          "bad",
		Type:        "insert",
		Namespace:   "throttle.test",
		EncodedBson: "",
	}
	err := applyOp(op, nil)
	assert.Error(t, err)
	assert.Equal(t, "Invalid ID: bad", err.Error())
}

func TestInvalidType(t *testing.T) {
	op := operation{
		ID:          bson.NewObjectId().Hex(),
		Type:        "badop",
		Namespace:   "throttle.test",
		EncodedBson: "",
	}
	err := applyOp(op, nil)
	assert.Error(t, err)
	assert.Equal(t, "Unknown type: badop", err.Error())
}

func TestInvalidEncodedBson(t *testing.T) {
	op := operation{
		ID:          bson.NewObjectId().Hex(),
		Type:        "insert",
		Namespace:   "throttle.test",
		EncodedBson: "",
	}
	err := applyOp(op, nil)
	assert.Error(t, err)
	assert.Equal(t, "Error unmarshaling bson Document is corrupted", err.Error())
}

func TestUpdate(t *testing.T) {
	db := setupDb(t)

	// Update a doc and check that it's changed
	obj := bson.M{"_id": bson.NewObjectId(), "key": "value"}
	assert.NoError(t, db.C("test").Insert(obj))

	updatedObj := bson.M{"key": "value2"}
	updateBytes, err := bson.Marshal(updatedObj)
	assert.NoError(t, err)

	op := operation{
		ID:          obj["_id"].(bson.ObjectId).Hex(),
		Type:        "update",
		Namespace:   "throttle.test",
		EncodedBson: base64.StdEncoding.EncodeToString(updateBytes),
	}
	assert.NoError(t, applyOp(op, db.Session))

	var result bson.M
	assert.NoError(t, db.C("test").Find(bson.M{}).One(&result))
	assert.Equal(t, "value2", result["key"].(string))

	// Try with the $set syntax
	setUpdate := bson.M{"$set": bson.M{"key": "value3"}}
	setBytes, err := bson.Marshal(setUpdate)
	assert.NoError(t, err)
	op.EncodedBson = base64.StdEncoding.EncodeToString(setBytes)

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
	bytes, err := bson.Marshal(obj)
	assert.NoError(t, err)

	op := operation{
		ID:          id.Hex(),
		Type:        "insert",
		Namespace:   "throttle.test",
		EncodedBson: base64.StdEncoding.EncodeToString(bytes),
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

	op := operation{
		ID:          id.Hex(),
		Type:        "remove",
		Namespace:   "throttle.test",
		EncodedBson: "",
	}
	assert.NoError(t, applyOp(op, db.Session))

	count, err := db.C("test").Count()
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}
