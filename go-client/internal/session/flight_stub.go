package session

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/flight"
	"github.com/apache/arrow/go/arrow/ipc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"
)

type FlightStub struct {
	session *Session

	stub flight.FlightServiceClient
}

func NewFlightStub(session *Session, host string, port string) (FlightStub, error) {
	stub, err := flight.NewClientWithMiddleware(
		net.JoinHostPort(host, port),
		nil,
		nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return FlightStub{}, err
	}

	return FlightStub{session: session, stub: stub}, nil
}

func (fs *FlightStub) SnapshotRecord(ctx context.Context, ticket *ticketpb2.Ticket) (array.Record, error) {
	ctx = fs.session.WithToken(ctx)

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
	rec1.Retain()
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

func (fs *FlightStub) ImportTable(ctx context.Context, rec array.Record) (TableHandle, error) {
	ctx = fs.session.WithToken(ctx)

	doPut, err := fs.stub.DoPut(ctx)
	if err != nil {
		return TableHandle{}, err
	}
	defer doPut.CloseSend()

	ticketNum := fs.session.NewTicketNum()

	descr := &flight.FlightDescriptor{Type: flight.FlightDescriptor_PATH, Path: []string{"export", strconv.Itoa(int(ticketNum))}}

	writer := flight.NewRecordWriter(doPut, ipc.WithSchema(rec.Schema()))

	writer.SetFlightDescriptor(descr)
	err = writer.Write(rec)
	if err != nil {
		return TableHandle{}, err
	}

	err = writer.Close()
	if err != nil {
		return TableHandle{}, err
	}

	_, err = doPut.Recv()
	if err != nil {
		return TableHandle{}, err
	}

	ticket := fs.session.MakeTicket(ticketNum)

	schema := rec.Schema()

	return NewTableHandle(fs.session, &ticket, schema, rec.NumRows(), true), nil
}
