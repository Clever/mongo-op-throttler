package apply

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
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
	if millisElapsed < 1700 || millisElapsed > 2300 {
		assert.Fail(t, fmt.Sprintf("Duration outside expected range %d", millisElapsed))
	}

	count, err := db.C("test").Count()
	assert.NoError(t, err)
	assert.Equal(t, 10, count)
}

func TestApplyOpErrorCases(t *testing.T) {

}

func TestUpdate(t *testing.T) {

}

func TestInsert(t *testing.T) {

}

func TestRemove(t *testing.T) {

}
