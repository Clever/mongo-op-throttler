package main

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTempFileFromPath(t *testing.T) {
	f, err := ioutil.TempFile("/tmp", "throttle-test")
	assert.NoError(t, err)
	defer os.RemoveAll(f.Name())

	_, err = f.Write([]byte("test data"))
	assert.NoError(t, err)

	file2name, err := tempFileFromPath(f.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(file2name)

	f2, err := os.Open(file2name)
	assert.NoError(t, err)

	buffer := make([]byte, 100)
	_, err = f2.Read(buffer)
	assert.NoError(t, err)
	// Test whether they're equal, trimming out of the extra stuff in the buffer
	assert.Equal(t, "test data", strings.Trim(string(buffer), "\x00"))
}
