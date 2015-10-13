package main

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

// Some way to parse...
// Some kind of typing???
type operation struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Namespace string `json:"namespace"`
	// TODO: Add some more details about what this could be. Tests for things like $set
	EncodedBson string `json:"base64bson"`
}

// TODO: Note that this is indempotent (this controls how we handle errors / upserts)
// TODO: Pass in the session??? Probably...
func Apply(ops io.Reader, opsPerSecond int, host string) error {
	opScanner := bufio.NewScanner(ops)

	// TODO: Explain why we don't use bulk

	session, err := mgo.Dial(host)
	if err != nil {
		return err
	}

	start := time.Now()
	numOps := 0

	for opScanner.Scan() {
		var op operation
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

// TODO: Add a nice comment!!!
func applyOp(op operation, session *mgo.Session) error {

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
		// TODO: Explain why we do this for idempotency of the whole
		// process
		if err == mgo.ErrNotFound {
			return nil
		}
		return err
	} else if op.Type == "remove" {
		return c.RemoveId(id)
	} else {
		return fmt.Errorf("Unknown type: %s", op.Type)
	}
}
