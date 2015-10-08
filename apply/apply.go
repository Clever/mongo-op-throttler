package apply

import (
	"io"

	"github.com/Clever/oplog-replay/ratecontroller"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// Some way to parse...
// Some kind of typing???
// Should this be a standard???
// SHould this be json???
type operation struct {
	ID          string
	Type        string
	EncodedBson string
}

func Apply(ops io.Reader, controller ratecontroller.Controller, host string) error {
	scanner := bsonScanner.New(ops)

	// TODO: Clean this up...
	var bulk mgo.Bulk

	for scanner.Scan() {
		// Unmarshal as we want
		var op operation

		addToOpSet()

		waitTime := controller.WaitTime()
		if waitTime == 0 {
			continue // Repeat the process
		}

		time.Sleep(waitTime * time.Milliseconds)

		results, err := bulk.Run()
		if err != nil {
			return err
		}
		bulk = mgo.Bulk{}
	}

	// TODO: See if we really need both these lines...
	if scanner.Err() != nil {
		return scanner.Err()
	}
	return nil
}

func addToOpSet(op operation, bulk mgo.Bulk) error {
	if op.Type == "insert" {
		bulk.Upsert(bson.M{"_id": op.ID}, op.EncodedBson)
	} else if op.Type == "update" {

	} else if op.Type == "delete" {
		// TODO: Will this work???
	}
}
