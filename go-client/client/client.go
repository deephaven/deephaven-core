package client

import (
	"context"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/deephaven/deephaven-core/go-client/internal/session"
)

// Client is just a wrapper around a Session to provide the correct interface.

// A client is the main way to interface with the Deephaven server.
//
// When finished, you should call `client.Close()`.
type Client struct {
	session session.Session
}

// Starts a new connection to a server.
//
// If running locally, the default host and port are "localhost" and "10000".
// If this returns successfully, the client should later be closed with client.Close().
func NewClient(ctx context.Context, host string, port string) (Client, error) {
	s, err := session.NewSession(ctx, host, port)
	if err != nil {
		return Client{}, err
	}

	return Client{session: s}, nil
}

// Creates a new empty table in the global scope.
//
// The table will have zero columns and the specified number of rows.
func (client *Client) EmptyTable(ctx context.Context, numRows int64) (session.TableHandle, error) {
	return client.session.EmptyTable(ctx, numRows)
}

// Uploads a table to the deephaven server.
//
// The table can then be manipulated and referenced using the returned TableHandle.
func (client *Client) ImportTable(ctx context.Context, rec array.Record) (session.TableHandle, error) {
	return client.session.ImportTable(ctx, rec)
}

// Binds a table reference to a given name so that it can be referenced by other clients or the web UI.
func (client *Client) BindToVariable(ctx context.Context, name string, table *session.TableHandle) error {
	return client.session.BindToVariable(ctx, name, table)
}

/* other basic API calls like opening tables will also go here... */

// Closes the connection to the server. Once closed, a client cannot perform any operations.
func (client *Client) Close() {
	// It's safe to call Close on a session multiple times so this is OK
	client.session.Close()
}
