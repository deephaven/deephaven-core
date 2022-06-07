package flight_stub

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/flight"
	"github.com/deephaven/deephaven-core/go-client/internal/conn_stub"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"
)

type FlightStub struct {
	conn conn_stub.ConnStub

	stub flight.FlightServiceClient
}

func NewFlightStub(conn conn_stub.ConnStub, host string, port string) (FlightStub, error) {
	stub, err := flight.NewClientWithMiddleware(
		net.JoinHostPort(host, port),
		nil,
		nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return FlightStub{}, err
	}

	return FlightStub{conn: conn, stub: stub}, nil
}

func (fs *FlightStub) SnapshotRecord(ctx context.Context, ticket *ticketpb2.Ticket) (array.Record, error) {
	ctx = fs.conn.WithToken(ctx)

	fticket := &flight.Ticket{Ticket: ticket.GetTicket()}

	req, err := fs.stub.DoGet(ctx, fticket)
	if err != nil {
		return nil, err
	}

	reader, err := flight.NewRecordReader(req)
	defer reader.Release()
	if err != nil {
		return nil, err
	}

	rec1, err := reader.Read()
	if err != nil {
		return nil, err
	}

	rec2, err := reader.Read()
	if err != io.EOF {
		rec1.Release()
		rec2.Release()
		return nil, errors.New("multiple records retrieved during snapshot")
	}

	return rec1, nil

}
