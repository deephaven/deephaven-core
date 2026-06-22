package client

import (
	"context"
	"io"
	"net"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	ticketpb2 "github.com/deephaven/deephaven-core/go/internal/proto/ticket"
)

// flightStub wraps Arrow Flight gRPC calls.
type flightStub struct {
	client *Client

	stub flight.Client // The stub for performing Arrow Flight gRPC requests.
}

func newFlightStub(client *Client, host string, port string) (*flightStub, error) {
	stub, err := flight.NewClientWithMiddleware(
		net.JoinHostPort(host, port),
		nil,
		nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	return &flightStub{client: client, stub: stub}, nil
}

// handshake creates a client for performing token handshakes with the Flight service.
// The client can be used to obtain and refresh authentication tokens.
func (fs *flightStub) handshake(ctx context.Context, opts ...grpc.CallOption) (flight.FlightService_HandshakeClient, error) {
	return fs.stub.Handshake(ctx, opts...)
}

// snapshotRecord downloads the data currently in the provided table and returns it as an Arrow Record.
// The server may split large snapshots across multiple Arrow record batches; all batches are read and
// concatenated into a single Record.
func (fs *flightStub) snapshotRecord(ctx context.Context, ticket *ticketpb2.Ticket) (arrow.Record, error) {
	ctx, err := fs.client.tokenMgr.withToken(ctx)
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
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	var batches []arrow.Record
	releaseBatches := func() {
		for _, b := range batches {
			b.Release()
		}
	}
	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			releaseBatches()
			return nil, err
		}
		rec.Retain()
		batches = append(batches, rec)
	}

	if len(batches) == 1 {
		return batches[0], nil
	}

	if len(batches) == 0 {
		// Static snapshots of empty tables produce zero record batches on the wire. Synthesize an
		// empty Record from the schema so callers can treat it uniformly.
		schema := reader.Schema()
		cols := make([]arrow.Array, len(schema.Fields()))
		for i, f := range schema.Fields() {
			b := array.NewBuilder(memory.DefaultAllocator, f.Type)
			cols[i] = b.NewArray()
			b.Release()
		}
		rec := array.NewRecord(schema, cols, 0)
		for _, c := range cols {
			c.Release()
		}
		return rec, nil
	}

	return concatRecords(batches)
}

// concatRecords merges multiple Arrow Records that share the same schema into a single Record by
// concatenating each column. The input batches are released before returning.
func concatRecords(batches []arrow.Record) (arrow.Record, error) {
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()

	schema := batches[0].Schema()
	numCols := int(batches[0].NumCols())
	var totalRows int64
	for _, b := range batches {
		totalRows += b.NumRows()
	}

	mergedCols := make([]arrow.Array, 0, numCols)
	for col := 0; col < numCols; col++ {
		arrs := make([]arrow.Array, len(batches))
		for i, b := range batches {
			arrs[i] = b.Column(col)
		}
		merged, err := array.Concatenate(arrs, memory.DefaultAllocator)
		if err != nil {
			return nil, err
		}
		defer merged.Release()
		mergedCols = append(mergedCols, merged)
	}

	return array.NewRecord(schema, mergedCols, totalRows), nil
}

// ImportTable uploads a table to the Deephaven server.
// The table can then be manipulated and referenced using the returned TableHandle.
func (fs *flightStub) ImportTable(ctx context.Context, rec arrow.Record) (*TableHandle, error) {
	ctx, err := fs.client.tokenMgr.withToken(ctx)
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
