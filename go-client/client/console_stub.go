package client

import (
	"context"

	consolepb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/console"
	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"
)

type consoleStub struct {
	client *Client

	stub consolepb2.ConsoleServiceClient

	consoleId *ticketpb2.Ticket
}

func NewConsoleStub(ctx context.Context, client *Client, sessionType string) (consoleStub, error) {
	ctx = client.WithToken(ctx)

	stub := consolepb2.NewConsoleServiceClient(client.GrpcChannel())

	reqTicket := client.NewTicket()

	req := consolepb2.StartConsoleRequest{ResultId: &reqTicket, SessionType: sessionType}
	resp, err := stub.StartConsole(ctx, &req)
	if err != nil {
		return consoleStub{}, err
	}

	consoleId := resp.ResultId

	return consoleStub{client: client, stub: stub, consoleId: consoleId}, nil
}

// Binds a table reference to a given name so that it can be referenced by other clients or the web UI.
func (console *consoleStub) BindToVariable(ctx context.Context, name string, table *TableHandle) error {
	ctx = console.client.WithToken(ctx)

	req := consolepb2.BindTableToVariableRequest{ConsoleId: console.consoleId, VariableName: name, TableId: table.ticket}
	_, err := console.stub.BindTableToVariable(ctx, &req)
	if err != nil {
		return err
	}

	return nil
}
