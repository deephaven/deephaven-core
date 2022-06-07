package console_stub

import (
	"context"

	"github.com/deephaven/deephaven-core/go-client/internal/conn_stub"

	consolepb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/console"
	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"
)

type ConsoleStub struct {
	conn conn_stub.ConnStub

	stub consolepb2.ConsoleServiceClient

	consoleId *ticketpb2.Ticket
}

func NewConsoleStub(ctx context.Context, conn conn_stub.ConnStub, sessionType string) (ConsoleStub, error) {
	ctx = conn.WithToken(ctx)

	stub := consolepb2.NewConsoleServiceClient(conn.GrpcChannel())

	reqTicket := conn.NewTicket()

	req := consolepb2.StartConsoleRequest{ResultId: &reqTicket, SessionType: sessionType}
	resp, err := stub.StartConsole(ctx, &req)
	if err != nil {
		return ConsoleStub{}, err
	}

	consoleId := resp.ResultId

	return ConsoleStub{conn: conn, stub: stub, consoleId: consoleId}, nil
}

func (console *ConsoleStub) BindToVariable(ctx context.Context, name string, tableTicket *ticketpb2.Ticket) error {
	ctx = console.conn.WithToken(ctx)

	req := consolepb2.BindTableToVariableRequest{ConsoleId: console.consoleId, VariableName: name, TableId: tableTicket}
	_, err := console.stub.BindTableToVariable(ctx, &req)
	if err != nil {
		return err
	}

	return nil
}
