package client

import (
	"context"
	"errors"
	"io"

	"github.com/apache/arrow/go/arrow/flight"
	"github.com/apache/arrow/go/arrow/memory"

	tablepb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/table"
)

type tableStub struct {
	client *Client

	stub tablepb2.TableServiceClient
}

func NewTableStub(client *Client) (tableStub, error) {
	stub := tablepb2.NewTableServiceClient(client.GrpcChannel())

	return tableStub{client: client, stub: stub}, nil
}

func (ts *tableStub) batch(ctx context.Context, ops []*tablepb2.BatchTableRequest_Operation) ([]TableHandle, error) {
	ctx = ts.client.WithToken(ctx)

	req := tablepb2.BatchTableRequest{Ops: ops}
	resp, err := ts.stub.Batch(ctx, &req)
	if err != nil {
		return nil, err
	}
	defer resp.CloseSend()

	exportedTables := []TableHandle{}

	for {
		created, err := resp.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		if !created.Success {
			return nil, errors.New(created.GetErrorInfo())
		}

		if _, ok := created.ResultId.Ref.(*tablepb2.TableReference_Ticket); ok {
			newTable, err := parseCreationResponse(ts.client, created)
			if err != nil {
				return nil, err
			}
			exportedTables = append(exportedTables, newTable)
		}
	}

	return exportedTables, nil
}

// Creates a new empty table in the global scope.
//
// The table will have zero columns and the specified number of rows.
func (ts *tableStub) EmptyTable(ctx context.Context, numRows int64) (TableHandle, error) {
	ctx = ts.client.WithToken(ctx)

	result := ts.client.NewTicket()

	req := tablepb2.EmptyTableRequest{ResultId: &result, Size: numRows}
	resp, err := ts.stub.EmptyTable(ctx, &req)
	if err != nil {
		return TableHandle{}, err
	}

	return parseCreationResponse(ts.client, resp)
}

func (ts *tableStub) DropColumns(ctx context.Context, table *TableHandle, cols []string) (TableHandle, error) {
	ctx = ts.client.WithToken(ctx)

	result := ts.client.NewTicket()

	source := tablepb2.TableReference{Ref: &tablepb2.TableReference_Ticket{Ticket: table.ticket}}

	req := tablepb2.DropColumnsRequest{ResultId: &result, SourceId: &source, ColumnNames: cols}
	resp, err := ts.stub.DropColumns(ctx, &req)
	if err != nil {
		return TableHandle{}, err
	}

	return parseCreationResponse(ts.client, resp)
}

func (ts *tableStub) Update(ctx context.Context, table *TableHandle, formulas []string) (TableHandle, error) {
	ctx = ts.client.WithToken(ctx)

	result := ts.client.NewTicket()

	source := tablepb2.TableReference{Ref: &tablepb2.TableReference_Ticket{Ticket: table.ticket}}

	req := tablepb2.SelectOrUpdateRequest{ResultId: &result, SourceId: &source, ColumnSpecs: formulas}
	resp, err := ts.stub.Update(ctx, &req)
	if err != nil {
		return TableHandle{}, err
	}

	return parseCreationResponse(ts.client, resp)
}

func parseCreationResponse(client *Client, resp *tablepb2.ExportedTableCreationResponse) (TableHandle, error) {
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

	return newTableHandle(client, respTicket, schema, resp.Size, resp.IsStatic), nil
}
