package shard

import (
	"bufio"
	"os"

	"github.com/uber/cadence/service/worker/scanner/executions/checks"
)

type (
	// CheckRequestConverterFn converts bytes to CheckRequest or returns error on failure
	CheckRequestConverterFn func([]byte) (*checks.CheckRequest, error)

	fileCheckRequestIterator struct {
		converter CheckRequestConverterFn

		// the following keep track of the iterator's state
		scanner *bufio.Scanner
		nextResult    *CheckRequestIteratorResult
		nextError     error
	}
)

// NewFileCheckRequestIterator constructs a fileCheckRequestIterator
func NewFileCheckRequestIterator(
	f *os.File,
	converter CheckRequestConverterFn,
) CheckRequestIterator {
	scanner := bufio.NewScanner(f)
	itr := &fileCheckRequestIterator{
		converter: converter,

		scanner: scanner,
		nextResult: nil,
		nextError: nil,
	}
	itr.advance()
	return itr
}

func (itr *fileCheckRequestIterator) Next() (*CheckRequestIteratorResult, error) {
	currResult := itr.nextResult
	currErr := itr.nextError
	if itr.HasNext() {
		itr.advance()
	}
	return currResult, currErr
}

func (itr *fileCheckRequestIterator) HasNext() bool {
	return itr.nextResult != nil && itr.nextError == nil
}

func (itr *fileCheckRequestIterator) advance() {
	hasNext := itr.scanner.Scan()
	if !hasNext {
		itr.nextResult = nil
		itr.nextError = itr.scanner.Err()
		if itr.nextError == nil {
			itr.nextError = ErrIteratorEmpty
		}
		return
	}
	cr, err := itr.converter(itr.scanner.Bytes())
	itr.nextResult = &CheckRequestIteratorResult{
		CheckRequest: cr,
		Error:        err,
	}
	itr.nextError = nil
}