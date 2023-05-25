package client

import (
	"context"
	sessionpb2 "github.com/deephaven/deephaven-core/go/internal/proto/session"
	ticketpb2 "github.com/deephaven/deephaven-core/go/internal/proto/ticket"
)

// sessionStub wraps gRPC calls from session.proto, which includes the session/keepalive handling.
type sessionStub struct {
	client *Client
	stub   sessionpb2.SessionServiceClient
}

// Performs the first handshake to get a client token.
func newSessionStub(client *Client) (*sessionStub, error) {
	stub := sessionpb2.NewSessionServiceClient(client.grpcChannel)

	hs := &sessionStub{
		client: client,
		stub:   stub,
	}

	return hs, nil
}

// release releases a table ticket to free its resources on the server.
func (hs *sessionStub) release(ctx context.Context, ticket *ticketpb2.Ticket) error {
	ctx, err := hs.client.tokenMgr.withToken(ctx)
	if err != nil {
		return err
	}

	req := sessionpb2.ReleaseRequest{Id: ticket}
	_, err = hs.stub.Release(ctx, &req)
	if err != nil {
		return err
	}
	return nil
}
