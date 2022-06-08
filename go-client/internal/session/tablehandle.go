package session

import (
	"context"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"

	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"
)

type TableHandle struct {
	Session  *Session
	Ticket   *ticketpb2.Ticket
	Schema   *arrow.Schema
	Size     int64
	IsStatic bool
}

func NewTableHandle(session *Session, ticket *ticketpb2.Ticket, schema *arrow.Schema, size int64, isStatic bool) TableHandle {
	return TableHandle{
		Session:  session,
		Ticket:   ticket,
		Schema:   schema,
		Size:     size,
		IsStatic: isStatic,
	}
}

// Downloads the current state of the table on the server and returns it as a Record.
//
// If a Record is returned successfully, it must be freed later with `record.Release()`
func (th *TableHandle) Snapshot(ctx context.Context) (array.Record, error) {
	return th.Session.snapshot(ctx, th)
}

// Returns a new table without the given columns.
func (th *TableHandle) DropColumns(ctx context.Context, cols []string) (TableHandle, error) {
	return th.Session.tableStub.DropColumns(ctx, th, cols)
}

// Returns a new table with additional columns calculated according to the formulas
func (th *TableHandle) Update(ctx context.Context, formulas []string) (TableHandle, error) {
	return th.Session.tableStub.Update(ctx, th, formulas)
}
