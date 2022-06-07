package tablehandle

import (
	"context"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/deephaven/deephaven-core/go-client/session"

	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"
)

type TableHandle struct {
	Session      *session.Session
	Ticket       *ticketpb2.Ticket
	SchemaHeader []byte
	Size         int64
	IsStatic     bool
}

func NewTableHandle(session *session.Session, ticket *ticketpb2.Ticket, schemaHeader []byte, size int64, isStatic bool) TableHandle {
	return TableHandle{
		Session:      session,
		Ticket:       ticket,
		SchemaHeader: schemaHeader,
		Size:         size,
		IsStatic:     isStatic,
	}
}

func (th *TableHandle) Snapshot(ctx context.Context) (array.Record, error) {
	return th.Session.SnapshotRecord(ctx, th.Ticket)
}
