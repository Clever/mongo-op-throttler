package convert

import (
	throttle "github.com/Clever/mongo-op-throttler"
	"gopkg.in/mgo.v2/bson"
)
// TODO: Add a nice comment!!!
// Note that this can return empty... if we don't understand the op
func convertOp(oplogEntry bson.M) (*throttle.Operation, error) {
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

	opObject, ok := oplogEntry["o"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Missing object field")
	}

	op := Operation{Namespace: namespace}

	switch opType {

	case "i":
		op.Type = "insert"
		op.ID, ok = opObject["_id"].(string)
		if !ok {
			return nil, fmt.Errorf("Insert missing 'o._id' field")
		}
		op.Object = opObject

	case "u":
		op.Type = "update"
		op.ID, ok = op["o2"].(map[string]interface{})["_id"].(string)
		if !ok {
			return nil, fmt.Errorf("Update missing o._id field")
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
		op.Object = opObject

		// Since this field is referenced in the Mongo applyCmd source code, but I haven't been able to
		// set it in any of our oplog entries, let's just sanity check that it isn't set.
		if _, ok = op["b"]; ok {
			return nil, fmt.Errorf("Unknown field 'b' in update")
		}

	case "d":
		op.Type = "remove"
		op.ID, ok = opObject["_id"].(string)
		if !ok {
			return nil, fmt.Errorf("Delete missing '_id' field")
		}

		// We see this on all our deletes so let's keep making sure it's there
		if b, ok := op["b"].(bool); !ok || !b {
			return nil, fmt.Errorf("'b' field not set to true for delete")
		}

	default:
		// It's theoretically possibly that is also 'c' or 'n', but we don't support them so let's error out
		return nil, fmt.Errorf("Unknown op type %s", opType)

	}

	return &op, nil
}
}
