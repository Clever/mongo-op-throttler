package convert

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
)

func TestConvertBsonBytes(t *testing.T) {
	doc := bson.M{
		"op": "i",
		"ns": "throttle.test",
		"o": bson.M{
			"_id": bson.NewObjectId(),
			"val": "value",
		},
	}
	bytes, err := bson.Marshal(doc)
	assert.NoError(t, err)

	op, err := OplogBytesToOp(bytes)
	assert.NoError(t, err)
	assert.Equal(t, "insert", op.Type)
}

func TestConvertInsertOp(t *testing.T) {
	obj := bson.M{
		"_id": "teacherId",
		"val": "value",
	}
	doc := bson.M{
		"v":  2,
		"op": "i",
		"ns": "archive.archive.teachers",
		"o":  obj,
	}

	op, err := oplogEntryToOp(doc)
	assert.NoError(t, err)
	assert.Equal(t, "insert", op.Type)
	assert.Equal(t, "teacherId", op.ID)
	assert.Equal(t, "archive.archive.teachers", op.Namespace)
	assert.Equal(t, obj, op.Obj)
}

func TestConvertRemoveOp(t *testing.T) {
	doc := bson.M{
		"v":  2,
		"op": "d",
		"ns": "archive.archive.students",
		"b":  true,
		"o": bson.M{
			"_id": "studentId",
		},
	}

	op, err := oplogEntryToOp(doc)
	assert.NoError(t, err)
	assert.Equal(t, "remove", op.Type)
	assert.Equal(t, "studentId", op.ID)
	assert.Equal(t, "archive.archive.students", op.Namespace)
}

func TestConvertUpdateOp(t *testing.T) {
	obj := bson.M{
		"$set": bson.M{
			"_permissions": []string{
				bson.NewObjectId().Hex(),
			},
		},
	}
	doc := bson.M{
		"v":  2,
		"op": "u",
		"ns": "test.sections",
		"o2": bson.M{
			"_id": "sectionId",
		},
		"o": obj,
	}

	op, err := oplogEntryToOp(doc)
	assert.NoError(t, err)
	assert.Equal(t, "update", op.Type)
	assert.Equal(t, "sectionId", op.ID)
	assert.Equal(t, "test.sections", op.Namespace)
	assert.Equal(t, obj, op.Obj)
}

func TestUnknownOp(t *testing.T) {
	doc := bson.M{
		"v":  2,
		"op": "c",
		"ns": "admin.$cmd",
		"o":  bson.M{"applyOps": []interface{}{}},
	}

	_, err := oplogEntryToOp(doc)
	assert.Error(t, err)
	assert.Equal(t, "Unknown op type c", err.Error())
}

func TestInvalidUpdateOperation(t *testing.T) {
	doc := bson.M{
		"op": "u",
		"ns": "test.sections",
		"o2": bson.M{
			"_id": "sectionId",
		},
		"o": bson.M{
			"$addToSet": bson.M{
				"_permissions": []string{
					bson.NewObjectId().Hex(),
				},
			},
		},
	}

	_, err := oplogEntryToOp(doc)
	assert.Error(t, err)
	assert.Equal(t, "Invalid key $addToSet in update object", err.Error())
}

func TestHandleObjectIdField(t *testing.T) {
	id := bson.NewObjectId()
	doc := bson.M{
		"v":  2,
		"op": "d",
		"ns": "test.students",
		"b":  true,
		"o": bson.M{
			"_id": id,
		},
	}

	op, err := oplogEntryToOp(doc)
	assert.NoError(t, err)
	assert.Equal(t, id.Hex(), op.ID)
}

func TestMissingFields(t *testing.T) {
	doc := bson.M{}
	_, err := oplogEntryToOp(doc)
	assert.Error(t, err)
}

func TestBFieldForRemove(t *testing.T) {
	doc := bson.M{
		"op": "d",
		"ns": "archive.archive.students",
		"b":  false,
		"o": bson.M{
			"_id": "studentId",
		},
	}

	_, err := oplogEntryToOp(doc)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "'b' field not set to true for delete")
}
