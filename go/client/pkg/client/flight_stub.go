package client

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/flight"
	"github.com/apache/arrow/go/v8/arrow/ipc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	ticketpb2 "github.com/deephaven/deephaven-core/go/client/internal/proto/ticket"
)

// flightStub wraps Arrow Flight gRPC calls.
type flightStub struct {
	client *Client

	stub flight.Client // The stub for performing Arrow Flight gRPC requests.
}

func newFlightStub(client *Client, host string, port string) (flightStub, error) {
	stub, err := flight.NewClientWithMiddleware(
		net.JoinHostPort(host, port),
		nil,
		nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return flightStub{}, err
	}

	return flightStub{client: client, stub: stub}, nil
}

// snapshotRecord downloads the data currently in the provided table and returns it as an Arrow Record.
func (fs *flightStub) snapshotRecord(ctx context.Context, ticket *ticketpb2.Ticket) (arrow.Record, error) {
	ctx, err := fs.client.withToken(ctx)
	if err != nil {
		return nil, err
	}

	fticket := &flight.Ticket{Ticket: ticket.GetTicket()}

	req, err := fs.stub.DoGet(ctx, fticket)
	if err != nil {
		return nil, err
	}
	defer req.CloseSend()

	reader, err := flight.NewRecordReader(req)
	defer reader.Release()
	if err != nil {
		return nil, err
	}

	rec1, err := reader.Read()
	if err != nil {
		return nil, err
	}
	rec1.Retain()

	rec2, err := reader.Read()
	if err != io.EOF {
		rec1.Release()
		rec2.Release()
		return nil, errors.New("multiple records retrieved during snapshot")
	}

	return rec1, nil
}

// ImportTable uploads a table to the Deephaven server.
// The table can then be manipulated and referenced using the returned TableHandle.
func (fs *flightStub) ImportTable(ctx context.Context, rec arrow.Record) (*TableHandle, error) {
	ctx, err := fs.client.withToken(ctx)
	if err != nil {
		return nil, err
	}

	doPut, err := fs.stub.DoPut(ctx)
	if err != nil {
		return nil, err
	}
	defer doPut.CloseSend()

	ticketNum := fs.client.ticketFact.nextId()

	//todo Seems like this should be a fixed size int cast here and not a generic int
	descr := &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{"export", strconv.Itoa(int(ticketNum))}}

	writer := flight.NewRecordWriter(doPut, ipc.WithSchema(rec.Schema()))

	writer.SetFlightDescriptor(descr)
	err = writer.Write(rec)
	if err != nil {
		return nil, err
	}

	err = writer.Close()
	if err != nil {
		return nil, err
	}

	_, err = doPut.Recv()
	if err != nil {
		return nil, err
	}

	ticket := fs.client.ticketFact.makeTicket(ticketNum)

	schema := rec.Schema()

	return newTableHandle(fs.client, &ticket, schema, rec.NumRows(), true), nil
}

// Close closes the flight stub and frees any associated resources.
// The flight stub should not be used after calling this function.
// The client lock should be held when calling this function.
func (fs *flightStub) Close() error {
	if fs.stub != nil {
		err := fs.stub.Close()
		if err != nil {
			return err
		}
		fs.stub = nil
	}
	return nil
}
