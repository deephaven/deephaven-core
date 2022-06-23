package client

import (
	"context"

	consolepb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/console"
	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"
)

// consoleStub wraps gRPC calls for console.proto.
type consoleStub struct {
	client *Client

	stub consolepb2.ConsoleServiceClient

	consoleId *ticketpb2.Ticket
}

func newConsoleStub(ctx context.Context, client *Client, sessionType string) (consoleStub, error) {
	ctx, err := client.withToken(ctx)
	if err != nil {
		return consoleStub{}, err
	}

	stub := consolepb2.NewConsoleServiceClient(client.grpcChannel)

	reqTicket := client.newTicket()

	req := consolepb2.StartConsoleRequest{ResultId: &reqTicket, SessionType: sessionType}
	resp, err := stub.StartConsole(ctx, &req)
	if err != nil {
		return consoleStub{}, err
	}

	consoleId := resp.ResultId

	return consoleStub{client: client, stub: stub, consoleId: consoleId}, nil
}

// BindToVariable binds a table reference to a given name so that it can be referenced by other clients or the web UI.
func (console *consoleStub) BindToVariable(ctx context.Context, name string, table *TableHandle) error {
	ctx, err := console.client.withToken(ctx)
	if err != nil {
		return err
	}

	req := consolepb2.BindTableToVariableRequest{ConsoleId: console.consoleId, VariableName: name, TableId: table.ticket}
	_, err = console.stub.BindTableToVariable(ctx, &req)
	if err != nil {
		return err
	}

	return nil
}

// RunScript directly uploads and executes a script on the deephaven server.
// The script language depends on the scriptLanguage argument passed when creating the client.
func (console *consoleStub) RunScript(ctx context.Context, script string) error {
	ctx, err := console.client.withToken(ctx)
	if err != nil {
		return err
	}

	req := consolepb2.ExecuteCommandRequest{ConsoleId: console.consoleId, Code: script}
	resp, err := console.stub.ExecuteCommand(ctx, &req)
	if err != nil {
		return err
	}

	console.client.handleScriptChanges(resp)

	return nil
}
