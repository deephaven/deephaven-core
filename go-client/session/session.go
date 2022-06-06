package session

import (
	"context"
	"net"

	sessionpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/session"

	"github.com/apache/arrow/go/arrow/flight"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// A session is the main way to interface with the Deephaven server.
type Session struct {
	grpcChannel *grpc.ClientConn

	token tokenManager

	flightStub  flight.Client
	sessionStub sessionpb2.SessionServiceClient
}

func NewSession(ctx context.Context, host string, port string) (Session, error) {
	grpcChannel, err := grpc.Dial(host+":"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return Session{}, err
	}

	sessionStub := sessionpb2.NewSessionServiceClient(grpcChannel)

	token, err := NewTokenManager(ctx, sessionStub)
	if err != nil {
		return Session{}, err
	}

	flightStub, err := flight.NewClientWithMiddleware(
		net.JoinHostPort(host, port),
		nil,
		nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return Session{}, err
	}

	return Session{
		grpcChannel: grpcChannel,

		token: token,

		flightStub:  flightStub,
		sessionStub: sessionStub,
	}, nil
}

/*
func (session *Session) ImportTable(ctx context.Context, table table.Table) error {
	ctx = session.withToken(ctx)

	doPut, err := session.flightStub.DoPut(ctx)
	if err != nil {
		return err
	}

	writer := flight.NewRecordWriter(doPut)
	defer writer.Close()

	tr := array.NewTableReader()
	defer tr.Release()

	err = writer.Write()
	if err != nil {
		return err
	}
}
*/

func (session *Session) Close(ctx context.Context) {
	session.token.Close()
	// TODO:
}

func (session *Session) withToken(ctx context.Context) context.Context {
	return metadata.NewOutgoingContext(context.Background(), metadata.Pairs("deephaven_session_id", string(session.token.Token())))
}
