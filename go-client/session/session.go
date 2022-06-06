package session

import (
	"context"
	"net"

	sessionpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/session"
	tablepb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/table"
	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"

	"github.com/deephaven/deephaven-core/go-client/internal/console"

	"github.com/apache/arrow/go/arrow/flight"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type Session struct {
	grpcChannel *grpc.ClientConn

	token tokenManager

	flightStub  flight.Client
	sessionStub sessionpb2.SessionServiceClient
	tableStub   tablepb2.TableServiceClient

	Console console.ConsoleStub

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

	session.tableStub = tablepb2.NewTableServiceClient(grpcChannel)
	session.Console, err = console.NewConsole(ctx, &session, "python") // TODO: session type
	if err != nil {
		// TODO: Close channel
		return Session{}, err
	}

	session.flightStub, err = flight.NewClientWithMiddleware(
		net.JoinHostPort(host, port),
		nil,
		nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
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
	num := session.NewTicketNum()

	bytes := []byte{'e', byte(num), byte(num >> 8), byte(num >> 16), byte(num >> 24)}

	return ticketpb2.Ticket{Ticket: bytes}
}

// Create a new table on the server with no columns and the specified number of rows
func (session *Session) EmptyTable(ctx context.Context, numRows int64) (*tablepb2.ExportedTableCreationResponse, error) {
	ctx = session.WithToken(ctx)

	ticket := session.NewTicket()

	req := tablepb2.EmptyTableRequest{ResultId: &ticket, Size: numRows}
	resp, err := session.tableStub.EmptyTable(ctx, &req)
	if err != nil {
		return &tablepb2.ExportedTableCreationResponse{}, err
	}

	return resp, nil
}

/*``
func (session *Session) ImportTable(ctx context.Context, table array.Table) error {
	ctx = session.withToken(ctx)

	doPut, err := session.flightStub.DoPut(ctx)
	if err != nil {
		return err
	}

	writer := flight.NewRecordWriter(doPut)
	defer writer.Close()

	tr := array.NewTableReader(table, 0)
	defer tr.Release()

	for tr.Next() {
		rec := tr.Record()

		fmt.Println("About to write ", rec)

		err = writer.Write(rec)
		if err != nil {
			return err
		}
	}

	return nil
}
*/

func (session *Session) Close() {
	session.token.Close()
	// TODO:
}

func (session *Session) WithToken(ctx context.Context) context.Context {
	return metadata.NewOutgoingContext(context.Background(), metadata.Pairs("deephaven_session_id", string(session.token.Token())))
}
