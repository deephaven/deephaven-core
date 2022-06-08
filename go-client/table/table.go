package table

import "github.com/deephaven/deephaven-core/go-client/internal/session"

// This is pretty much nothing but a module visibility hack because I don't know how to use Go.
// This should be made just a TableHandle eventually.
type Table struct {
	session.TableHandle
}
