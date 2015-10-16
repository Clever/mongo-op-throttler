package convert

import (
	"fmt"
	"strings"

	"github.com/Clever/mongo-op-throttler/operation"
	"gopkg.in/mgo.v2/bson"
)

// TODO: Add a nice comment!!!
// Note that this can return empty... if we don't understand the op
func OplogEntryToOp(oplogEntry bson.M) (*operation.Op, error) {
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

	opObject, ok := oplogEntry["o"].(bson.M)
	if !ok {
		return nil, fmt.Errorf("Missing object field")
	}

	op := operation.Op{Namespace: namespace}

	switch opType {

	case "i":
		op.Type = "insert"
		id, ok := opObject["_id"]
		if !ok {
			return nil, fmt.Errorf("Insert missing or 'o._id' field")
		}

		var err error
		op.ID, err = convertIdToString(id)
		if err != nil {
			return nil, err
		}
		op.Obj = opObject

	case "u":
		op.Type = "update"
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
		for key := range opObject {
			if strings.Contains(key, "$") && key != "$set" && key != "$unset" {
				return nil, fmt.Errorf("Invalid key %s in update object", key)
			}
		}
		op.Obj = opObject

		// Since this field is referenced in the Mongo applyCmd source code, but I haven't been able to
		// set it in any of our oplog entries, let's just sanity check that it isn't set.
		if _, ok = oplogEntry["b"]; ok {
			return nil, fmt.Errorf("Unknown field 'b' in update")
		}

	case "d":
		op.Type = "remove"
		id, ok := opObject["_id"]
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

	default:
		// It's theoretically possibly that is also 'c' or 'n', but we don't support them so let's error out
		return nil, fmt.Errorf("Unknown op type %s", opType)

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
