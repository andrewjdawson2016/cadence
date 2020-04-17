package util

import (
	"errors"
	"os"
)

type (
	blobstoreBufferedWriter struct {}
)

// NewBlobstoreBufferedWriter constructs a blobstoreBufferedWriter
func NewBlobstoreBufferedWriter(f *os.File, flushThreshold int) BufferedWriter {
	return &blobstoreBufferedWriter{}
}

// Add adds a new entity
func (w *blobstoreBufferedWriter) Add(e interface{}) error {
	return errors.New("not yet implemented")
}

// Flush flushes the buffer
func (w *blobstoreBufferedWriter) Flush() error {
	return errors.New("not yet implemented")
}