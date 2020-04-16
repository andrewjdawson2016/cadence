package util

type (
	// BufferedWriter buffers and writes entities to a sink
	BufferedWriter interface {
		Add(interface{}) error
		Flush() error
	}
)
