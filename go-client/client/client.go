package client

import (
	"context"
	"errors"

	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

var ErrClosedClient = errors.New("client is closed")

type Client struct {
	grpcChannel *grpc.ClientConn

	sessionStub
	consoleStub
	flightStub
	tableStub

	nextTicket int32
}

// Starts a connection to the deephaven server.
//
// The client should be closed using `Close()` after it is done being used.
//
// Note that the provided context is saved and used to send keepalive messages.
func NewClient(ctx context.Context, host string, port string) (*Client, error) {
	grpcChannel, err := grpc.Dial(host+":"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	client := &Client{grpcChannel: grpcChannel}

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

	client.consoleStub, err = NewConsoleStub(ctx, client, "python") // TODO: client type
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

func (client *Client) NewTicketNum() int32 {
	client.nextTicket += 1
	if client.nextTicket <= 0 {
		// TODO:
		panic("out of tickets")
	}

	return client.nextTicket
}

func (client *Client) NewTicket() ticketpb2.Ticket {
	id := client.NewTicketNum()

	return client.MakeTicket(id)
}

func (client *Client) MakeTicket(id int32) ticketpb2.Ticket {
	bytes := []byte{'e', byte(id), byte(id >> 8), byte(id >> 16), byte(id >> 24)}

	return ticketpb2.Ticket{Ticket: bytes}
}

func (client *Client) ExecQuery(ctx context.Context, nodes ...QueryNode) ([]TableHandle, error) {
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
