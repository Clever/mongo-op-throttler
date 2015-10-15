package apply

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// operation is the definition of the mongo command to run
type Operation struct {
	ID string `json:"id"`
	// Valid types are: 'insert', 'update' or 'remove'
	Type string `json:"type"`
	// The namespace as defined by mongo. For example, "clever.events"
	Namespace   string `json:"namespace"`
	EncodedBson string `json:"base64bson"`
}

// applyOps applies all the operations in the io.Reader to the specified
// database session at the specified speed.
// Note that applyOps is idempotent so it can be run repeatedly. It does
// this by doing things like converting inserts into upserts. For more details
// so the applyOp code.
func ApplyOps(r io.Reader, opsPerSecond int, session *mgo.Session) error {
	opScanner := bufio.NewScanner(r)

	start := time.Now()
	numOps := 0

	for opScanner.Scan() {
		var op Operation
		if err := json.Unmarshal(opScanner.Bytes(), &op); err != nil {
			return fmt.Errorf("Error parsing json: %s", err.Error())
		}

		millisElapsed := time.Now().Sub(start).Nanoseconds() / (1000 * 1000)
		expectedMillisElapsed := (float64(numOps) / float64(opsPerSecond)) * 1000

		timeToWait := int64(expectedMillisElapsed) - millisElapsed
		if timeToWait > 0 {
			time.Sleep(time.Duration(timeToWait) * time.Millisecond)
		}

		if err := applyOp(op, session); err != nil {
			return err
		}
		numOps++
	}

	return opScanner.Err()
}

// applyOp applies a single operation to a database. Note that we apply a single
// op at a time instead of using the mgo bulk library. There are two motivations for that:
// 1. The bulk library doesn't support remove yet, so we would have to special case that
// 2. The bulk operation only operates on a single collection at a time so we would have
// to break it apart.
// Given these limitations, it seemed like just applying them serially was meaningfully
// simpler, and in testing we could get close to 1K ops per second applying them serially,
// so we decided that was good enough for now and we could revisit later if we needed more speed.
func applyOp(op Operation, session *mgo.Session) error {

	splitNamespace := strings.SplitN(op.Namespace, ".", 2)
	if len(splitNamespace) != 2 {
		return fmt.Errorf("Invalid namespace: %s", op.Namespace)
	}

	if !bson.IsObjectIdHex(op.ID) {
		return fmt.Errorf("Invalid ID: %s", op.ID)
	}
	id := bson.ObjectIdHex(op.ID)

	var objBson bson.M
	bsonBytes, err := base64.StdEncoding.DecodeString(op.EncodedBson)
	if err != nil {
		return err
	}
	if op.Type == "insert" || op.Type == "update" {
		if err := bson.Unmarshal(bsonBytes, &objBson); err != nil {
			return fmt.Errorf("Error unmarshaling bson %s", err.Error())
		}
	}

	c := session.DB(splitNamespace[0]).C(splitNamespace[1])

	if op.Type == "insert" {
		_, err := c.UpsertId(id, objBson)
		return err
	} else if op.Type == "update" {
		err := c.UpdateId(id, objBson)
		// Don't error on mgo not found because we want to support idempotency
		// and the document could have been removed in a previous run
		if err == mgo.ErrNotFound {
			return nil
		}
		return err
	} else if op.Type == "remove" {
		err := c.RemoveId(id)
		// Don't error on mgo not found because we want to support idempotency
		// and the document could have been removed in a previous run
		if err == mgo.ErrNotFound {
			return nil
		}
		return err
	} else {
		return fmt.Errorf("Unknown type: %s", op.Type)
	}
}
