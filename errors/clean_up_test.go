package errors

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

type errCallable struct{ error }

func (e errCallable) Func() error                   { return e.error }
func (e errCallable) FuncCtx(context.Context) error { return e.error }

func TestCleanUp(t *testing.T) {
	const (
		closeMsg  = "close [seuozr]"
		returnMsg = "return [mntbnb]"
	)

	for callIdx, call := range []func(errCallable, *error){
		func(e errCallable, err *error) { CleanUp(e.Func, err) },
		func(e errCallable, err *error) { CleanUpCtx(context.Background(), e.FuncCtx, err) },
	} {
		t.Run(strconv.Itoa(callIdx), func(t *testing.T) {
			// No return error, no close error.
			gotErr := func() (err error) {
				e := errCallable{}
				defer call(e, &err)
				return nil
			}()
			assert.NoError(t, gotErr)

			// No return error, close error.
			gotErr = func() (err error) {
				e := errCallable{errors.New(closeMsg)}
				defer call(e, &err)
				return nil
			}()
			assert.Equal(t, gotErr.Error(), closeMsg)

			// Return error, no close error.
			gotErr = func() (err error) {
				e := errCallable{}
				defer call(e, &err)
				return errors.New(returnMsg)
			}()
			assert.Equal(t, gotErr.Error(), returnMsg)

			// Return error, close error.
			gotErr = func() (err error) {
				e := errCallable{errors.New(closeMsg)}
				defer call(e, &err)
				return errors.New(returnMsg)
			}()
			assert.Contains(t, gotErr.Error(), returnMsg)
			assert.Contains(t, gotErr.Error(), closeMsg)
		})
	}
}
