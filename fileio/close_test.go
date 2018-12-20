package fileio_test

import (
	"testing"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/fileio"
	"github.com/stretchr/testify/assert"
)

type errFile struct {
	err error
}

func (f *errFile) String() string { return f.err.Error() }

func (f *errFile) Close() error {
	return f.err
}

func TestCloseAndReport(t *testing.T) {
	closeMsg := "close [seuozr]"
	returnMsg := "return [mntbnb]"

	// No return error, no close error.
	gotErr := func() (err error) {
		f := errFile{}
		defer fileio.CloseAndReport(&f, &err)
		return nil
	}()
	assert.NoError(t, gotErr)

	// No return error, close error.
	gotErr = func() (err error) {
		f := errFile{errors.New(closeMsg)}
		defer fileio.CloseAndReport(&f, &err)
		return nil
	}()
	assert.Equal(t, gotErr.Error(), closeMsg)

	// Return error, no close error.
	gotErr = func() (err error) {
		f := errFile{}
		defer fileio.CloseAndReport(&f, &err)
		return errors.New(returnMsg)
	}()
	assert.Equal(t, gotErr.Error(), returnMsg)

	// Return error, close error.
	gotErr = func() (err error) {
		f := errFile{errors.New(closeMsg)}
		defer fileio.CloseAndReport(&f, &err)
		return errors.New(returnMsg)
	}()
	assert.Contains(t, gotErr.Error(), returnMsg)
	assert.Contains(t, gotErr.Error(), closeMsg)
}
