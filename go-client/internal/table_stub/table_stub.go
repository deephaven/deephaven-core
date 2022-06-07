package table_stub

import (
	"context"

	"github.com/deephaven/deephaven-core/go-client/internal/conn_stub"

	tablepb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/table"
)

type TableStub struct {
	conn conn_stub.ConnStub

	stub tablepb2.TableServiceClient
}

func NewTableStub(conn conn_stub.ConnStub) (TableStub, error) {
	stub := tablepb2.NewTableServiceClient(conn.GrpcChannel())

	return TableStub{conn: conn, stub: stub}, nil
}

// Create a new table on the server with no columns and the specified number of rows
func (ts *TableStub) EmptyTable(ctx context.Context, numRows int64) (*tablepb2.ExportedTableCreationResponse, error) {
	ctx = ts.conn.WithToken(ctx)

	ticket := ts.conn.NewTicket()

	req := tablepb2.EmptyTableRequest{ResultId: &ticket, Size: numRows}
	resp, err := ts.stub.EmptyTable(ctx, &req)
	if err != nil {
		return &tablepb2.ExportedTableCreationResponse{}, err
	}

	return resp, nil
}
