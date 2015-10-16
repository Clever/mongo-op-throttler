package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/CLever/mongo-op-throttler/apply"
	"github.com/Clever/pathio"

	"gopkg.in/mgo.v2"
)

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
	defer f.Close()

	if err = apply.ApplyOps(f, *opsPerSecond, session); err != nil {
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
