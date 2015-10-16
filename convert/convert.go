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
		return nil, fmt.Errorf("Error parsing json: %s", err.Error())
	}

	return oplogEntryToOp(bsonOp)
}

// oplogEntryToOp converts from bson.M to operation.Op
func oplogEntryToOp(oplogEntry bson.M) (*operation.Op, error) {
	// Note that this has only been tested for the Mongo 2.4 format

	// Based on the logic from the source code:
	// https://github.com/mongodb/mongo/blob/v2.4/src/mongo/db/oplog.cpp#L791
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
		// It's theoretically possibly that is also 'c' or 'n', but we don't support them so let's error out
		return nil, fmt.Errorf("Unknown op type %s", opType)
	}
}

func convertToInsert(namespace string, obj bson.M) (*operation.Op, error) {
	op := operation.Op{Namespace: namespace, Type: "insert"}
	id, ok := obj["_id"]
	if !ok {
		return nil, fmt.Errorf("Insert missing or 'o._id' field")
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

	// Since this field is referenced in the Mongo applyCmd source code, but I haven't been able to
	// set it in any of our oplog entries, let's just sanity check that it isn't set.
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

	// We see this on all our deletes so let's keep making sure it's there
	if b, ok := oplogEntry["b"].(bool); !ok || !b {
		return nil, fmt.Errorf("'b' field not set to true for delete")
	}
	return &op, nil
}

func convertIdToString(id interface{}) (string, error) {
	if str, ok := id.(string); ok {
		return str, nil
	}
	if objId, ok := id.(bson.ObjectId); ok {
		return objId.Hex(), nil
	}
	return "", fmt.Errorf("Unknown id field %s", id)
}
