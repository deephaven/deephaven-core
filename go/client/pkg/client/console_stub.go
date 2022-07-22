package client

import (
	"context"
	"errors"

	consolepb2 "github.com/deephaven/deephaven-core/go/client/internal/proto/console"
	ticketpb2 "github.com/deephaven/deephaven-core/go/client/internal/proto/ticket"
)

// ErrNoConsole is returned by
var ErrNoConsole = errors.New("the client was not started with console support (see WithConsole)")

// consoleStub wraps gRPC calls for console.proto.
type consoleStub struct {
	client *Client

	stub consolepb2.ConsoleServiceClient // The stub for console.proto gRPC requests.

	// A consoleId assigned to this client by the server.
	// If it is nil, scripts cannot be run.
	consoleId *ticketpb2.Ticket
}

// newConsoleStub creates a console stub.
//
// If sessionType is non-empty, it will start a console for use with scripts.
// The sessionType determines what language the scripts will use. It can be either "python" or "groovy" and must match the server language.
func newConsoleStub(ctx context.Context, client *Client, sessionType string) (consoleStub, error) {
	ctx, err := client.withToken(ctx)
	if err != nil {
		return consoleStub{}, err
	}

	stub := consolepb2.NewConsoleServiceClient(client.grpcChannel)

	var consoleId *ticketpb2.Ticket
	if sessionType != "" {
		reqTicket := client.ticketFact.newTicket()

		req := consolepb2.StartConsoleRequest{ResultId: &reqTicket, SessionType: sessionType}
		resp, err := stub.StartConsole(ctx, &req)
		if err != nil {
			return consoleStub{}, err
		}

		consoleId = resp.ResultId
	}

	return consoleStub{client: client, stub: stub, consoleId: consoleId}, nil
}

// BindToVariable binds a table reference to a given name on the server so that it can be referenced by other clients or the web UI.
//
// If WithConsole was not passed when creating the client, this will return ErrNoConsole.
func (console *consoleStub) BindToVariable(ctx context.Context, name string, table *TableHandle) error {
	ctx, err := console.client.withToken(ctx)
	if err != nil {
		return err
	}

	if console.consoleId == nil {
		return ErrNoConsole
	}

	req := consolepb2.BindTableToVariableRequest{ConsoleId: console.consoleId, VariableName: name, TableId: table.ticket}
	_, err = console.stub.BindTableToVariable(ctx, &req)
	if err != nil {
		return err
	}

	return nil
}
