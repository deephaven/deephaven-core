package client

import (
	"context"
	"errors"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/flight"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/deephaven/deephaven-core/go-client/session"

	"github.com/deephaven/deephaven-core/go-client/tablehandle"
	//tablepb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/table"
)

// A client is the main way to interface with the Deephaven server.
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

func (client *Client) EmptyTable(ctx context.Context, numRows int64) (tablehandle.TableHandle, error) {
	resp, err := client.session.EmptyTable(ctx, numRows)
	if err != nil {
		return tablehandle.TableHandle{}, err
	}

	if !resp.Success {
		return tablehandle.TableHandle{}, errors.New("server error: `" + resp.GetErrorInfo() + "`")
	}

	respTicket := resp.ResultId.GetTicket()
	if respTicket == nil {
		return tablehandle.TableHandle{}, errors.New("server response did not have ticket")
	}

	alloc := memory.NewGoAllocator()
	schema, err := flight.DeserializeSchema(resp.SchemaHeader, alloc)
	if err != nil {
		return tablehandle.TableHandle{}, err
	}

	return tablehandle.NewTableHandle(client.session, respTicket, schema, resp.Size, resp.IsStatic), nil
}

func (client *Client) ImportTable(ctx context.Context, rec array.Record) (tablehandle.TableHandle, error) {
	ticket, err := client.session.ImportTable(ctx, rec)
	if err != nil {
		return tablehandle.TableHandle{}, err
	}

	schema := rec.Schema()

	return tablehandle.NewTableHandle(client.session, ticket, schema, rec.NumRows(), true), nil
}

func (client *Client) BindToVariable(ctx context.Context, name string, table tablehandle.TableHandle) error {
	return client.session.BindToVariable(ctx, name, table.Ticket)
}

func (client *Client) Close() {
	client.session.Close()
}
