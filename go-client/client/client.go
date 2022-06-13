package client

import (
	"context"
	"errors"

	applicationpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/application"
	consolepb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/console"
	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

var ErrClosedClient = errors.New("client is closed")

type fieldId struct {
	appId     string
	fieldName string
}

type Client struct {
	grpcChannel *grpc.ClientConn

	sessionStub
	consoleStub
	flightStub
	tableStub

	nextTicket int32

	tables map[fieldId]*TableHandle
}

type SyncOption int

const (
	NoSync        SyncOption = iota
	SyncOnce      SyncOption = iota
	SyncRepeating SyncOption = iota
)

// Starts a connection to the deephaven server.
//
// scriptLanguage can be either "python" or "groovy", and must match the language used on the server. Python is the default.
//
// The client should be closed using `Close()` after it is done being used.
//
// Note that the provided context is saved and used to send keepalive messages.
func NewClient(ctx context.Context, host string, port string, scriptLanguage string) (*Client, error) {
	grpcChannel, err := grpc.Dial(host+":"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	client := &Client{grpcChannel: grpcChannel, tables: make(map[fieldId]*TableHandle)}

	client.sessionStub, err = NewSessionStub(ctx, client)
	if err != nil {
		client.Close()
		return nil, err
	}

	client.tableStub, err = NewTableStub(client)
	if err != nil {
		client.Close()
		return nil, err
	}

	client.consoleStub, err = NewConsoleStub(ctx, client, scriptLanguage)
	if err != nil {
		client.Close()
		return nil, err
	}

	client.flightStub, err = NewFlightStub(client, host, port)
	if err != nil {
		client.Close()
		return nil, err
	}

	return client, nil
}

func (client *Client) GrpcChannel() *grpc.ClientConn {
	return client.grpcChannel
}

func (client *Client) newTicketNum() int32 {
	client.nextTicket += 1
	if client.nextTicket <= 0 {
		// TODO:
		panic("out of tickets")
	}

	return client.nextTicket
}

func (client *Client) newTicket() ticketpb2.Ticket {
	id := client.newTicketNum()

	return client.makeTicket(id)
}

func (client *Client) makeTicket(id int32) ticketpb2.Ticket {
	bytes := []byte{'e', byte(id), byte(id >> 8), byte(id >> 16), byte(id >> 24)}

	return ticketpb2.Ticket{Ticket: bytes}
}

// Sends an entire query over to the server all at once and returns the resulting tables.
//
// If this function completes successfully, the number of tables returned will always match the
// number of query nodes passed.
//
// This may return a QueryError if the query is invalid.
func (client *Client) ExecQuery(ctx context.Context, nodes ...QueryNode) ([]*TableHandle, error) {
	return execQuery(client, ctx, nodes)
}

func (client *Client) Close() {
	client.sessionStub.Close()
	if client.grpcChannel != nil {
		client.grpcChannel.Close()
		client.grpcChannel = nil
	}
}

func (client *Client) WithToken(ctx context.Context) context.Context {
	return metadata.NewOutgoingContext(context.Background(), metadata.Pairs("deephaven_session_id", string(client.Token())))
}

func (client *Client) handleScriptChanges(resp *consolepb2.ExecuteCommandResponse) {
	client.handleFieldChanges(resp.Changes)
}

func (client *Client) handleFieldChanges(resp *applicationpb2.FieldsChangeUpdate) {
	for _, created := range resp.Created {
		if created.TypedTicket.Type == "Table" {
			fieldId := fieldId{appId: created.ApplicationId, fieldName: created.FieldName}
			client.tables[fieldId] = newTableHandle(client, created.TypedTicket.Ticket, nil, 0, false)
		}
	}

	for _, updated := range resp.Updated {
		if updated.TypedTicket.Type == "Table" {
			fieldId := fieldId{appId: updated.ApplicationId, fieldName: updated.FieldName}
			client.tables[fieldId] = newTableHandle(client, updated.TypedTicket.Ticket, nil, 0, false)
		}
	}

	for _, removed := range resp.Removed {
		if removed.TypedTicket.Type == "Table" {
			fieldId := fieldId{appId: removed.ApplicationId, fieldName: removed.FieldName}
			delete(client.tables, fieldId)
		}
	}
}
