package convert

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
)

func TestConvertInsertOp(t *testing.T) {
	doc := bson.M{
		"v":  2,
		"op": "i",
		"ns": "archive.archive.teachers",
		"o": bson.M{
			"_id": "teacherId",
			"val": "55d57fd49e8a1b0d007f73b4",
		},
	}

	op, err := ConvertOplogEntryToOp(doc)
	assert.NoError(t, err)
	assert.Equal(t, "insert", op.Type)
	assert.Equal(t, "teacherId", op.ID)
	assert.Equal(t, "archive.archive.teachers", op.Namespace)
	// TODO: The actual object??? Here and elsewhere...
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

	op, err := ConvertOplogEntryToOp(doc)
	assert.NoError(t, err)
	assert.Equal(t, "remove", op.Type)
	assert.Equal(t, "studentId", op.ID)
	assert.Equal(t, "archive.archive.students", op.Namespace)
}

func TestConvertUpdateOp(t *testing.T) {
	doc := bson.M{
		"v":  2,
		"op": "u",
		"ns": "clever.sections",
		"o2": bson.M{
			"_id": "sectionId",
		},
		"o": bson.M{
			"$set": bson.M{
				"_permissions": []string{
					"532a5db5c69b239f0d000026",
				},
			},
		},
	}

	op, err := ConvertOplogEntryToOp(doc)
	assert.NoError(t, err)
	assert.Equal(t, "update", op.Type)
	assert.Equal(t, "sectionId", op.ID)
	assert.Equal(t, "clever.sections", op.Namespace)
}

func TestUnknownOp(t *testing.T) {
	doc := bson.M{
		"v":  2,
		"op": "c",
		"ns": "admin.$cmd",
		"o":  bson.M{"applyOps": []interface{}{}},
	}

	_, err := ConvertOplogEntryToOp(doc)
	assert.Error(t, err)
	assert.Equal(t, "Unknown op type c", err.Error())
}

func TestInvalidUpdateOperation(t *testing.T) {
	doc := bson.M{
		"op": "u",
		"ns": "clever.sections",
		"o2": bson.M{
			"_id": "sectionId",
		},
		"o": bson.M{
			"$addToSet": bson.M{
				"_permissions": []string{
					"532a5db5c69b239f0d000026",
				},
			},
		},
	}

	_, err := ConvertOplogEntryToOp(doc)
	assert.Error(t, err)
	assert.Equal(t, "Invalid key $addToSet in update object", err.Error())
}

func TestMissingFields(t *testing.T) {
	doc := bson.M{}
	_, err := ConvertOplogEntryToOp(doc)
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

	_, err := ConvertOplogEntryToOp(doc)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "'b' field not set to true for delete")
}
