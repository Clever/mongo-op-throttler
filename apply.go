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

// Some way to parse...
// Some kind of typing???
type operation struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
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

	f, err := tempFileFromPath(*path)
	if err != nil {
		log.Fatalf("Error creating temp file from path %s", err)
	}
	defer os.RemoveAll(f.Name())

	if err = applyOps(f, *opsPerSecond, session); err != nil {
		log.Fatalf("Error applying ops %s", err)
	}
}

// TODO: Add a nice comment about why we do this whole dance
// Note that it needs to be removed and closed when done???
func tempFileFromPath(path string) (*os.File, error) {
	f, err := ioutil.TempFile("/tmp", "throttler")
	if err != nil {
		return nil, fmt.Errorf("Error creating temporary file %s", err)
	}

	reader, err := pathio.Reader(path)
	if err != nil {
		return nil, fmt.Errorf("Error reading from the path %s", err)
	}
	defer reader.Close()

	if _, err = io.Copy(f, reader); err != nil {
		return nil, fmt.Errorf("Error copying the data from s3 %s", err)
	}
	f.Close()

	return os.Open(f.Name())
}

// TODO: Note that this is indempotent (this controls how we handle errors / upserts)
// TODO: Pass in the session??? Probably...
func applyOps(r io.Reader, opsPerSecond int, session *mgo.Session) error {
	opScanner := bufio.NewScanner(r)

	// TODO: Explain why we don't use bulk

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
		err := c.RemoveId(id)
		if err == mgo.ErrNotFound {
			return nil
		}
		return err
	} else {
		return fmt.Errorf("Unknown type: %s", op.Type)
	}
}
