package apply

import (
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/Clever/mongo-op-throttler/convert"
	"github.com/Clever/mongo-op-throttler/operation"
	// Use custom scanner with higher length limitation
	bsonScanner "github.com/Clever/mongo-op-throttler/bson"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// applyOps applies all the operations in the io.Reader to the specified
// database session at the specified speed.
// Note that applyOps is idempotent so it can be run repeatedly. It does
// this by doing things like converting inserts into upserts. For more details
// so the applyOp code.
func ApplyOps(r io.Reader, opsPerSecond float64, session *mgo.Session) error {
	log.Printf("Beginning to replay")
	opScanner := bsonScanner.New(r)

	start := time.Now()
	numOps := 0

	for opScanner.Scan() {

		op, err := convert.OplogBytesToOp(opScanner.Bytes())
		if err != nil {
			return fmt.Errorf("Error interpreting oplog entry %s", err.Error())
		}
		// It is possible for an op to be a no-op, but not an error. For example an index creation
		if op == nil {
			continue
		}

		millisElapsed := time.Now().Sub(start).Nanoseconds() / (1000 * 1000)
		expectedMillisElapsed := (float64(numOps) / opsPerSecond) * 1000

		timeToWait := int64(expectedMillisElapsed) - millisElapsed
		if timeToWait > 0 {
			time.Sleep(time.Duration(timeToWait) * time.Millisecond)
		}

		if err := applyOp(*op, session); err != nil {
			return err
		}
		numOps++

		if numOps%1000 == 0 {
			log.Printf("Processed %d ops", numOps)
		}
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
func applyOp(op operation.Op, session *mgo.Session) error {
	splitNamespace := strings.SplitN(op.Namespace, ".", 2)
	if len(splitNamespace) != 2 {
		return fmt.Errorf("Invalid namespace: %s", op.Namespace)
	}

	if !bson.IsObjectIdHex(op.ID) {
		return fmt.Errorf("Invalid ID: %s", op.ID)
	}
	id := bson.ObjectIdHex(op.ID)

	c := session.DB(splitNamespace[0]).C(splitNamespace[1])

	switch op.Type {
	case "insert":
		_, err := c.UpsertId(id, op.Obj)
		return err

	case "update":
		err := c.UpdateId(id, op.Obj)
		// Don't error on mgo not found because we want to support idempotency
		// and the document could have been removed in a previous run
		// See https://github.com/mongodb/docs/commit/238d6755a74c3c978cc272d318283f726379a43c
		// for more details. Ideally we would turn this into a upsert, but we can't do that
		// until we get Mongo 2.6 oplogs (2.4 ones don't have enough of the document to do an
		// upsert)
		if err == mgo.ErrNotFound {
			return nil
		}
		return err

	case "remove":
		err := c.RemoveId(id)
		// Don't error on mgo not found because we want to support idempotency
		// and the document could have been removed in a previous run
		if err == mgo.ErrNotFound {
			return nil
		}
		return err

	default:
		return fmt.Errorf("Unknown type: %s", op.Type)
	}
}
