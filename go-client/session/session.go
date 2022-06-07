package session

import (
	"context"

	"github.com/deephaven/deephaven-core/go-client/internal/console_stub"
	"github.com/deephaven/deephaven-core/go-client/internal/flight_stub"
	"github.com/deephaven/deephaven-core/go-client/internal/table_stub"

	sessionpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/session"
	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type Session struct {
	grpcChannel *grpc.ClientConn

	token tokenManager

	sessionStub sessionpb2.SessionServiceClient

	console_stub.ConsoleStub
	flight_stub.FlightStub
	table_stub.TableStub

	nextTicket int32
}

// Starts a connection to the deephaven server.
//
// The session should be closed using `Close()` after it is done being used.
//
// Note that the provided context is saved and used to send keepalive messages.
func NewSession(ctx context.Context, host string, port string) (Session, error) {
	grpcChannel, err := grpc.Dial(host+":"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return Session{}, err
	}

	session := Session{grpcChannel: grpcChannel}

	session.sessionStub = sessionpb2.NewSessionServiceClient(grpcChannel)
	session.token, err = NewTokenManager(ctx, session.sessionStub)
	if err != nil {
		// TODO: Close channel
		return Session{}, err
	}

	session.TableStub, err = table_stub.NewTableStub(&session)
	if err != nil {
		// TODO: Close channel
		return Session{}, err
	}

	session.ConsoleStub, err = console_stub.NewConsoleStub(ctx, &session, "python") // TODO: session type
	if err != nil {
		// TODO: Close channel
		return Session{}, err
	}

	session.FlightStub, err = flight_stub.NewFlightStub(&session, host, port)
	if err != nil {
		// TODO: Close channel
		return Session{}, err
	}

	return session, nil
}

func (session *Session) GrpcChannel() *grpc.ClientConn {
	return session.grpcChannel
}

func (session *Session) NewTicketNum() int32 {
	session.nextTicket += 1
	if session.nextTicket <= 0 {
		// TODO:
		panic("out of tickets")
	}

	return session.nextTicket
}

func (session *Session) NewTicket() ticketpb2.Ticket {
	id := session.NewTicketNum()

	return session.MakeTicket(id)
}

func (session *Session) MakeTicket(id int32) ticketpb2.Ticket {
	bytes := []byte{'e', byte(id), byte(id >> 8), byte(id >> 16), byte(id >> 24)}

	return ticketpb2.Ticket{Ticket: bytes}
}

func (session *Session) Close() {
	session.token.Close()
	// TODO:
}

func (session *Session) WithToken(ctx context.Context) context.Context {
	return metadata.NewOutgoingContext(context.Background(), metadata.Pairs("deephaven_session_id", string(session.token.Token())))
}
