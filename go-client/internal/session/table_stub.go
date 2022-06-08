package session

import (
	"context"
	"errors"

	"github.com/apache/arrow/go/arrow/flight"
	"github.com/apache/arrow/go/arrow/memory"

	tablepb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/table"
)

type TableStub struct {
	session *Session

	stub tablepb2.TableServiceClient
}

func NewTableStub(session *Session) (TableStub, error) {
	stub := tablepb2.NewTableServiceClient(session.GrpcChannel())

	return TableStub{session: session, stub: stub}, nil
}

func (ts *TableStub) EmptyTable(ctx context.Context, numRows int64) (TableHandle, error) {
	ctx = ts.session.WithToken(ctx)

	result := ts.session.NewTicket()

	req := tablepb2.EmptyTableRequest{ResultId: &result, Size: numRows}
	resp, err := ts.stub.EmptyTable(ctx, &req)
	if err != nil {
		return TableHandle{}, err
	}

	return parseCreationResponse(ts.session, resp)
}

func (ts *TableStub) DropColumns(ctx context.Context, table *TableHandle, cols []string) (TableHandle, error) {
	ctx = ts.session.WithToken(ctx)

	result := ts.session.NewTicket()

	source := tablepb2.TableReference{Ref: &tablepb2.TableReference_Ticket{Ticket: table.Ticket}}

	req := tablepb2.DropColumnsRequest{ResultId: &result, SourceId: &source, ColumnNames: cols}
	resp, err := ts.stub.DropColumns(ctx, &req)
	if err != nil {
		return TableHandle{}, err
	}

	return parseCreationResponse(ts.session, resp)
}

func (ts *TableStub) Update(ctx context.Context, table *TableHandle, formulas []string) (TableHandle, error) {
	ctx = ts.session.WithToken(ctx)

	result := ts.session.NewTicket()

	source := tablepb2.TableReference{Ref: &tablepb2.TableReference_Ticket{Ticket: table.Ticket}}

	req := tablepb2.SelectOrUpdateRequest{ResultId: &result, SourceId: &source, ColumnSpecs: formulas}
	resp, err := ts.stub.Update(ctx, &req)
	if err != nil {
		return TableHandle{}, err
	}

	return parseCreationResponse(ts.session, resp)
}

func parseCreationResponse(session *Session, resp *tablepb2.ExportedTableCreationResponse) (TableHandle, error) {
	if !resp.Success {
		return TableHandle{}, errors.New("server error: `" + resp.GetErrorInfo() + "`")
	}

	respTicket := resp.ResultId.GetTicket()
	if respTicket == nil {
		return TableHandle{}, errors.New("server response did not have ticket")
	}

	alloc := memory.NewGoAllocator()
	schema, err := flight.DeserializeSchema(resp.SchemaHeader, alloc)
	if err != nil {
		return TableHandle{}, err
	}

	return NewTableHandle(session, respTicket, schema, resp.Size, resp.IsStatic), nil
}
