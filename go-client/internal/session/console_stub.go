package session

import (
	"context"

	consolepb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/console"
	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"
)

type ConsoleStub struct {
	session *Session

	stub consolepb2.ConsoleServiceClient

	consoleId *ticketpb2.Ticket
}

func NewConsoleStub(ctx context.Context, session *Session, sessionType string) (ConsoleStub, error) {
	ctx = session.WithToken(ctx)

	stub := consolepb2.NewConsoleServiceClient(session.GrpcChannel())

	reqTicket := session.NewTicket()

	req := consolepb2.StartConsoleRequest{ResultId: &reqTicket, SessionType: sessionType}
	resp, err := stub.StartConsole(ctx, &req)
	if err != nil {
		return ConsoleStub{}, err
	}

	consoleId := resp.ResultId

	return ConsoleStub{session: session, stub: stub, consoleId: consoleId}, nil
}

func (console *ConsoleStub) BindToVariable(ctx context.Context, name string, table *TableHandle) error {
	ctx = console.session.WithToken(ctx)

	req := consolepb2.BindTableToVariableRequest{ConsoleId: console.consoleId, VariableName: name, TableId: table.ticket}
	_, err := console.stub.BindTableToVariable(ctx, &req)
	if err != nil {
		return err
	}

	return nil
}
