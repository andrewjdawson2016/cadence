package common

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
)

type (
	fileCheckRequestIterator struct {
		// the following keep track of the iterator's state
		scanner *bufio.Scanner
		nextResult    *CheckRequestIteratorResult
		nextError     error
	}
)

// NewFileCheckRequestIterator constructs a CheckRequestIterator backed by an os.File.
// This CheckRequestIterator assumes the file given can be read from.
// The responsibility to close the file belongs to the caller.
// This CheckRequestIterator assumes the file contains newline separated entities,
//where each entity can be json unmarshalled into a ScanOutputEntity.
func NewFileCheckRequestIterator(f *os.File) CheckRequestIterator {
	scanner := bufio.NewScanner(f)
	itr := &fileCheckRequestIterator{
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
	defer func() {
		if !ValidCheckRequestIteratorResult(itr.nextResult) {
			itr.nextResult = &CheckRequestIteratorResult{
				CheckRequest: CheckRequest{},
				Error:        errors.New("iterator entered invalid state"),
			}
		}
	}()
	hasNext := itr.scanner.Scan()
	if !hasNext {
		itr.nextResult = nil
		itr.nextError = itr.scanner.Err()
		if itr.nextError == nil {
			itr.nextError = ErrIteratorEmpty
		}
		return
	}
	line := itr.scanner.Bytes()
	var soe ScanOutputEntity
	if err := json.Unmarshal(line, &soe); err != nil {
		itr.nextResult = &CheckRequestIteratorResult{
			CheckRequest: CheckRequest{},
			Error:        err,
		}
	}



	cr, err := itr.converter(itr.scanner.Bytes())
	itr.nextResult = &CheckRequestIteratorResult{
		CheckRequest: cr,
		Error:        err,
	}
	itr.nextError = nil
}