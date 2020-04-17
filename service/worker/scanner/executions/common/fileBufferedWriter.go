package common

import (
	"encoding/json"
	"os"
	"strings"
)

type (
	fileBufferedWriter struct {
		f              *os.File
		entries        []interface{}
		flushThreshold int
	}
)

// NewFileBufferedWriter constructs a BufferedWriter backed by os.File.
// This BufferedWriter assumes the file given can be written to.
// The responsibility to close the file belongs to the caller.
// Added entities are serialized using json and outputted to file such that each entity is separated by a newline.
func NewFileBufferedWriter(f *os.File, flushThreshold int) BufferedWriter {
	return &fileBufferedWriter{
		f:              f,
		flushThreshold: flushThreshold,
	}
}

// Add adds a new entity
func (w *fileBufferedWriter) Add(e interface{}) error {
	if len(w.entries) > w.flushThreshold {
		if err := w.Flush(); err != nil {
			return err
		}
	}
	w.entries = append(w.entries, e)
	return nil
}

// Flush flushes the buffer
func (w *fileBufferedWriter) Flush() error {
	var builder strings.Builder
	for _, e := range w.entries {
		if err := w.writeToBuilder(&builder, e); err != nil {
			return err
		}
	}
	if err := w.writeBuilderToFile(&builder, w.f); err != nil {
		return err
	}
	w.entries = nil
	return nil
}

func (w *fileBufferedWriter) writeToBuilder(builder *strings.Builder, e interface{}) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	builder.WriteString(string(data))
	builder.WriteString("\r\n")
	return nil
}

func (w *fileBufferedWriter) writeBuilderToFile(builder *strings.Builder, f *os.File) error {
	_, err := f.WriteString(builder.String())
	return err
}
