package convert

import (
	"fmt"
	"strings"

	"github.com/Clever/mongo-op-throttler/operation"
	"gopkg.in/mgo.v2/bson"
)

// OplogBytesToOp converts the raw bytes for an oplog into a Mongo operation
// as defined by operation.Op. There are two reasons we don't immediately write
// the oplog entries to the database.
// 1. Keeps the logic for understanding oplogs separate from the rest of the code
// 2. Makes it easier to have a worker that takes in a file of operation.Ops instead
// of the oplog
func OplogBytesToOp(raw []byte) (*operation.Op, error) {
	var bsonOp bson.M
	if err := bson.Unmarshal(raw, &bsonOp); err != nil {
		return nil, fmt.Errorf("Error parsing bson: %s", err.Error())
	}

	return oplogEntryToOp(bsonOp)
}

// oplogEntryToOp converts from bson.M to operation.Op
// Note that this has only been tested for the Mongo 2.4 format
// Based on the logic from the source code:
// https://github.com/mongodb/mongo/blob/v2.4/src/mongo/db/oplog.cpp#L791
//
// oplog entries have the following format:
// "ts" : The timestampe of the entry
// "h" :
// "v" : The version of the oplog
// "op": "i", "d", "u", ... as detailed here:
//   https://github.com/mongodb/mongo/blob/801d87f5c8d66d5f5a462c5e0daae67e6b848976/src/mongo/db/oplog.cpp#L147-L162
// "ns" : The namespace (for example, "clever.sections")
// "o" : The object to be insert, the update command, or the document to be removed
// "o2" : Only applies
// "b" : Means "justOne" for removes, and "upsert" on updates. "justOne" is always set for removes for
//   reasons described below. Upsert doesn't seem to be set (AFACT) on updates, again the details are
//   described below.
// There are a few fields that don't apply to inserts, updates, or removes (the only ops we handle)
//
// How oplog entries are created:
// If the user does an insert then Mongo will create an "op" : "i" entry in the oplog
// If the user does an upsert, if the document is already in the database Mongo will create an "op" : "u" entry.
//   Otherwise it will create an "op" : "i" oplog entry.
// If the user does an update then Mongo will create an oplog entry for every document actually updated
// If the user does a remove then Mongo will create one "op" : "d" entry for each document actually removed
//   and since each oplog entry only represents one op, "b" will be set to "justOne"
func oplogEntryToOp(oplogEntry bson.M) (*operation.Op, error) {
	v, ok := oplogEntry["v"].(int)
	if !ok {
		return nil, fmt.Errorf("Missing version")
	}
	// For now we only support oplogs with a version 2
	if v != 2 {
		return nil, fmt.Errorf("Convert only supports version 2, got %d", v)
	}

	opType, ok := oplogEntry["op"].(string)
	if !ok {
		return nil, fmt.Errorf("Missing op type")
	}
	namespace, ok := oplogEntry["ns"].(string)
	if !ok {
		return nil, fmt.Errorf("Missing namespace")
	}

	// Ignore changes to the system namespace. These are things like system.indexes
	if strings.HasPrefix(namespace, "system.") {
		return nil, nil
	}

	obj, ok := oplogEntry["o"].(bson.M)
	if !ok {
		return nil, fmt.Errorf("Missing object field")
	}

	switch opType {
	case "i":
		return convertToInsert(namespace, obj)
	case "u":
		return convertToUpdate(namespace, obj, oplogEntry)
	case "d":
		return convertToRemove(namespace, obj, oplogEntry)
	default:
		// It's theoretically possibly that is also 'c', 'n', or 'db', but we don't support them so
		// let's error out.
		return nil, fmt.Errorf("Unknown op type %s", opType)
	}
}

func convertToInsert(namespace string, obj bson.M) (*operation.Op, error) {
	op := operation.Op{Namespace: namespace, Type: "insert"}
	id, ok := obj["_id"]
	if !ok {
		// This is valid for indexes, so let's see if the key field exists, and if so assume it's an index
		if _, ok := obj["key"]; ok {
			return nil, nil
		}
		return nil, fmt.Errorf("Insert missing or 'o._id' field %#v\n", obj)
	}

	var err error
	op.ID, err = convertIdToString(id)
	if err != nil {
		return nil, err
	}
	op.Obj = obj
	return &op, nil
}

func convertToUpdate(namespace string, obj, oplogEntry bson.M) (*operation.Op, error) {
	op := operation.Op{Namespace: namespace, Type: "update"}
	id, ok := oplogEntry["o2"].(bson.M)["_id"]
	if !ok {
		return nil, fmt.Errorf("Update missing o._id field")
	}

	var err error
	op.ID, err = convertIdToString(id)
	if err != nil {
		return nil, err
	}

	// Check to make sure the object only has $ fields we understand
	// Note that other Mongo update commands (afaict) are converted to either direct
	// set commands or $set and $unset commnands. For example an $addToSet command
	// becoomes {"$set" : {"key.1" : "value"}}
	for key := range obj {
		if strings.Contains(key, "$") && key != "$set" && key != "$unset" {
			return nil, fmt.Errorf("Invalid key %s in update object", key)
		}
	}
	op.Obj = obj

	// Technically cmd.applyOp supports "upserts" on updates ("b" -> "upsert"), but AFAICT
	// they never come from oplogs. See comment for oplogEntryToOp for details.
	if _, ok = oplogEntry["b"]; ok {
		return nil, fmt.Errorf("Unknown field 'b' in update")
	}
	return &op, nil
}

func convertToRemove(namespace string, obj, oplogEntry bson.M) (*operation.Op, error) {
	op := operation.Op{Namespace: namespace, Type: "remove"}
	id, ok := obj["_id"]
	if !ok {
		return nil, fmt.Errorf("Delete missing '_id' field")
	}

	var err error
	op.ID, err = convertIdToString(id)
	if err != nil {
		return nil, err
	}

	// "b" stands for "justOne" on deletes. It is always true for oplogs for reasons detailed
	// in the oplogEntryToOp comments.
	if b, ok := oplogEntry["b"].(bool); !ok || !b {
		return nil, fmt.Errorf("'b' field not set to true for delete")
	}
	return &op, nil
}

// convertIdToString converts the id field to a string. In Mongo the _id field can be anything as
// long as its unique. For now we just support strings and bson.ObjectIds. We can add more support
// if it becomes necessary.
func convertIdToString(id interface{}) (string, error) {
	switch t := id.(type) {
	case string:
		return t, nil
	case bson.ObjectId:
		return t.Hex(), nil
	default:
		return "", fmt.Errorf("Unknown id field %s", id)
	}
}
