package common

import (
	"errors"
	"os"
)

type (
	blobstoreBufferedWriter struct{}
)

// NewBlobstoreBufferedWriter constructs a BufferedWriter backed by blobstore
func NewBlobstoreBufferedWriter(f *os.File, flushThreshold int) BufferedWriter {
	return &blobstoreBufferedWriter{}
}

// Add adds a new entity
func (w *blobstoreBufferedWriter) Add(_ interface{}) error {
	return errors.New("not yet implemented")
}

// Flush flushes the buffer
func (w *blobstoreBufferedWriter) Flush() error {
	return errors.New("not yet implemented")
}
