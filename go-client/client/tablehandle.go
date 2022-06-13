package client

import (
	"context"
	"log"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"

	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"
)

// A reference to a table stored on the deephaven server.
//
// It should be eventually released using Release once it is no longer needed.
type TableHandle struct {
	client   *Client
	ticket   *ticketpb2.Ticket
	schema   *arrow.Schema
	size     int64
	isStatic bool
}

func newTableHandle(client *Client, ticket *ticketpb2.Ticket, schema *arrow.Schema, size int64, isStatic bool) *TableHandle {
	return &TableHandle{
		client:   client,
		ticket:   ticket,
		schema:   schema,
		size:     size,
		isStatic: isStatic,
	}
}

// Returns true if this table does not change over time.
// This will be false for things like streaming tables or timetables.
func (th *TableHandle) IsStatic() bool {
	return th.isStatic
}

// Downloads the current state of the table on the server and returns it as a Record.
//
// If a Record is returned successfully, it must be freed later with `record.Release()`
func (th *TableHandle) Snapshot(ctx context.Context) (array.Record, error) {
	return th.client.snapshotRecord(ctx, th.ticket)
}

// Creates a new query based on this table. Table operations can be performed on query nodes,
//
func (th *TableHandle) Query() QueryNode {
	qb := newQueryBuilder(th.client, th)
	return qb.curRootNode()
}

// Releases this table handle's resources on the server. The TableHandle is no longer usable after Release is called.
func (th *TableHandle) Release(ctx context.Context) {
	if th.client != nil {
		err := th.client.release(ctx, th.ticket)
		if err != nil {
			log.Println("unable to release table:", err.Error())
		}

		th.client = nil
		th.ticket = nil
		th.schema = nil
	}
}
