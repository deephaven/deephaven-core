package client

import (
	"context"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/deephaven/deephaven-core/go-client/internal/session"
)

// A client is the main way to interface with the Deephaven server.
//
// When finished, you should call `client.Close()`.
type Client struct {
	session *session.Session
}

func NewClient(ctx context.Context, host string, port string) (Client, error) {
	s, err := session.NewSession(ctx, host, port)
	if err != nil {
		return Client{}, err
	}

	return Client{session: &s}, nil
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
func (client *Client) BindToVariable(ctx context.Context, name string, table session.TableHandle) error {
	return client.session.BindToVariable(ctx, name, table)
}

// Closes the connection to the server. Once closed, a client cannot perform any operations.
func (client *Client) Close() {
	if client.session != nil {
		client.session.Close()
		client.session = nil
	}
}
