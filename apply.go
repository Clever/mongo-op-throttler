package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Clever/pathio"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// operation is the definition of the mongo command to run
type operation struct {
	ID string `json:"id"`
	// Valid types are: 'insert', 'update' or 'remove'
	Type string `json:"type"`
	// The namespace as defined by mongo. For example, "clever.events"
	Namespace   string `json:"namespace"`
	EncodedBson string `json:"base64bson"`
}

func main() {
	mongoURL := flag.String("mongoURL", "localhost", "The mongo database to run the operations against")
	path := flag.String("path", "", "The path to the json operations to replay")
	opsPerSecond := flag.Int("speed", 1, "The number of operations to apply per second")
	flag.Parse()

	session, err := mgo.Dial(*mongoURL)
	if err != nil {
		log.Fatalf("Failed to connect to Mongo %s", err)
	}
	defer session.Close()

	filename, err := tempFileFromPath(*path)
	if err != nil {
		log.Fatalf("Error creating temp file from path %s", err)
	}
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Error opening file back up %s", err)
	}
	defer os.RemoveAll(filename)

	if err = applyOps(f, *opsPerSecond, session); err != nil {
		log.Fatalf("Error applying ops %s", err)
	}
}

// tempFileFromPath takes in an arbitrary path and uses pathio to write it to a
// temporary file and passes back the location of that temporary file. We use it
// because we've had problems in the past where we stream data from s3 and the stream
// randomly breaks in the middle of processing, so we want to download the full
// file before doing any other processing.
func tempFileFromPath(path string) (string, error) {
	f, err := ioutil.TempFile("/tmp", "throttler")
	if err != nil {
		return "", fmt.Errorf("Error creating temporary file %s", err)
	}
	defer f.Close()

	reader, err := pathio.Reader(path)
	if err != nil {
		return "", fmt.Errorf("Error reading from the path %s", err)
	}
	defer reader.Close()

	if _, err = io.Copy(f, reader); err != nil {
		return "", fmt.Errorf("Error copying the data from s3 %s", err)
	}
	return f.Name(), nil
}

// applyOps applies all the operations in the io.Reader to the specified
// database session at the specified speed.
// Note that applyOps is idempotent so it can be run repeatedly. It does
// this by doing things like converting inserts into upserts. For more details
// so the applyOp code.
func applyOps(r io.Reader, opsPerSecond int, session *mgo.Session) error {
	opScanner := bufio.NewScanner(r)

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

// applyOp applies a single operation to a database. Note that we apply a single
// op at a time instead of using the mgo bulk library. There are two motivations for that:
// 1. The bulk library doesn't support remove yet, so we would have to special case that
// 2. The bulk operation only operates on a single collection at a time so we would have
// to break it apart.
// Given these limitations, it seemed like just applying them serially was meaningfully
// simpler, and in testing we could get close to 1K ops per second applying them serially,
// so we decided that was good enough for now and we could revisit later if we needed more speed.
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
