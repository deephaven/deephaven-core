package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/flight"
	"github.com/apache/arrow/go/v8/arrow/ipc"
	flatbuffers "github.com/google/flatbuffers/go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	ticketpb2 "github.com/deephaven/deephaven-core/go/internal/proto/ticket"

	flatbuf_b "github.com/deephaven/deephaven-core/go/internal/barrage/flatbuf"
	flatbuf_a "github.com/deephaven/deephaven-core/go/org/apache/arrow/flatbuf"

	"github.com/deephaven/deephaven-core/go/pkg/client/ticking"
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

type subscribeOptions struct {
	useDeephavenNulls bool
	minUpdateInterval time.Duration
	batchSize         int32
}

func newSubscribeOptions(opts ...SubscribeOption) subscribeOptions {
	options := subscribeOptions{
		useDeephavenNulls: false,
		minUpdateInterval: time.Duration(0),
		batchSize:         4096, // recommended in BarrageSubscriptionOptions
	}

	for _, opt := range opts {
		opt.apply(&options)
	}

	return options
}

type SubscribeOption interface {
	apply(opts *subscribeOptions)
}

type funcSubscribeOption struct {
	f func(opts *subscribeOptions)
}

func (opt funcSubscribeOption) apply(opts *subscribeOptions) {
	opt.f(opts)
}

func WithDeephavenNulls() SubscribeOption {
	return funcSubscribeOption{f: func(opts *subscribeOptions) {
		opts.useDeephavenNulls = true
	}}
}

// Duration only has a resolution of milliseconds.
func WithMinUpdateInterval(interval time.Duration) SubscribeOption {
	if interval.Milliseconds() == 0 {
		panic("interval too short (must be at least one millisecond!)")
	}

	if interval.Milliseconds() > math.MaxInt32 {
		panic("interval too long (must fit within an int32)")
	}

	return funcSubscribeOption{f: func(opts *subscribeOptions) {
		opts.minUpdateInterval = interval
	}}
}

func WithBatchSize(batchSize int32) SubscribeOption {
	return funcSubscribeOption{f: func(opts *subscribeOptions) {
		opts.batchSize = batchSize
	}}
}

func (fs *flightStub) subscribe(ctx context.Context, handle *TableHandle, options ...SubscribeOption) (*ticking.TickingTable, <-chan ticking.TickingStatus, error) {
	opts := newSubscribeOptions(options...)

	ctx, err := fs.client.withToken(ctx)
	if err != nil {
		return nil, nil, err
	}

	doExchg, err := fs.stub.DoExchange(ctx)
	if err != nil {
		return nil, nil, err
	}

	// TODO: Viewport support
	/*ser := ticking.NewRowSetSerializer()
	//ser.AddRowRange(0, 9)
	ser.AddRowRange(0, 4)
	ser.AddRowRange(10, 14)
	viewportSet := ser.Finish()*/

	builder_b := flatbuffers.NewBuilder(0)

	flatbuf_b.BarrageSubscriptionOptionsStart(builder_b)
	flatbuf_b.BarrageSubscriptionOptionsAddUseDeephavenNulls(builder_b, opts.useDeephavenNulls)
	flatbuf_b.BarrageSubscriptionOptionsAddBatchSize(builder_b, opts.batchSize)
	flatbuf_b.BarrageSubscriptionOptionsAddMinUpdateIntervalMs(builder_b, int32(opts.minUpdateInterval.Milliseconds()))
	barrageOpts := flatbuf_b.BarrageSubscriptionOptionsEnd(builder_b)

	flatbuf_b.BarrageSubscriptionRequestStartTicketVector(builder_b, len(handle.ticket.Ticket))
	for i := len(handle.ticket.Ticket) - 1; i >= 0; i-- {
		builder_b.PrependByte(handle.ticket.Ticket[i])
	}
	ticketVec := builder_b.EndVector(len(handle.ticket.Ticket))
	/*flatbuf_b.BarrageSubscriptionRequestStartColumnsVector(builder_b, 0)
	columnVec := builder_b.EndVector(0)*/

	/*flatbuf_b.BarrageSubscriptionRequestStartViewportVector(builder_b, 0)
	for i := len(viewportSet) - 1; i >= 0; i-- {
		builder_b.PrependByte(viewportSet[i])
	}
	viewportVec := builder_b.EndVector(len(viewportSet))*/

	flatbuf_b.BarrageSubscriptionRequestStart(builder_b)
	flatbuf_b.BarrageSubscriptionRequestAddTicket(builder_b, ticketVec)
	//flatbuf_b.BarrageSubscriptionRequestAddColumns(builder_b, columnVec)
	//flatbuf_b.BarrageSubscriptionRequestAddViewport(builder_b, viewportVec)
	flatbuf_b.BarrageSubscriptionRequestAddReverseViewport(builder_b, false)
	flatbuf_b.BarrageSubscriptionRequestAddSubscriptionOptions(builder_b, barrageOpts)
	bsrOff := flatbuf_b.BarrageSubscriptionRequestEnd(builder_b)
	builder_b.Finish(bsrOff)
	bsrPayload := builder_b.FinishedBytes()

	builder_c := flatbuffers.NewBuilder(0)
	flatbuf_b.BarrageMessageWrapperStartMsgPayloadVector(builder_c, len(bsrPayload))
	for i := len(bsrPayload) - 1; i >= 0; i-- {
		builder_c.PrependByte(bsrPayload[i])
	}
	bsrPayloadVec := builder_c.EndVector(len(bsrPayload))
	flatbuf_b.BarrageMessageWrapperStart(builder_c)
	flatbuf_b.BarrageMessageWrapperAddMagic(builder_c, 0x6E687064)
	flatbuf_b.BarrageMessageWrapperAddMsgType(builder_c, flatbuf_b.BarrageMessageTypeBarrageSubscriptionRequest)
	flatbuf_b.BarrageMessageWrapperAddMsgPayload(builder_c, bsrPayloadVec)
	bmwOff := flatbuf_b.BarrageMessageWrapperEnd(builder_c)
	builder_c.Finish(bmwOff)
	customMeta := builder_c.FinishedBytes()

	builder_a := flatbuffers.NewBuilder(0)
	flatbuf_a.MessageStart(builder_a)
	flatbuf_a.MessageAddHeaderType(builder_a, flatbuf_a.MessageHeaderNONE)
	msgOff := flatbuf_a.MessageEnd(builder_a)
	builder_a.Finish(msgOff)
	msgBuf := builder_a.FinishedBytes()

	desc := &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: []byte{0x64, 0x70, 0x68, 0x6E}}
	data := &flight.FlightData{DataHeader: msgBuf, AppMetadata: customMeta, FlightDescriptor: desc}

	err = doExchg.Send(data)
	if err != nil {
		return nil, nil, err
	}

	reader, err := flight.NewRecordReader(doExchg)
	if err != nil {
		return nil, nil, err
	}

	tbl, err := ticking.NewTickingTable(handle.schema)
	if err != nil {
		return nil, nil, err
	}

	updateChan := make(chan ticking.TickingStatus, 1)

	go func() {
		defer doExchg.CloseSend()
		defer close(updateChan)

		var pendingUpdate *ticking.TickingUpdate
		var pendingRecords []arrow.Record

		trySubmitUpdate := func() (done bool, err error) {
			var expectedRows uint64
			if pendingUpdate.HasIncludedRows {
				expectedRows += pendingUpdate.AddedRowsIncluded.Size()
			} else {
				expectedRows += pendingUpdate.AddedRows.Size()
			}

			if len(pendingUpdate.ModifiedRows) > 0 {
				// TODO: Again, ragged edge problems.
				// expectedRows should actually be a per-column check,
				// or we should send padded records.
				expectedRows += pendingUpdate.ModifiedRows[0].Size()
			}

			var rowsReceived uint64
			for _, rec := range pendingRecords {
				rowsReceived += uint64(rec.NumRows())
			}

			if rowsReceived > expectedRows {
				return false, errors.New("received too many rows")
			} else if rowsReceived == expectedRows {
				pendingUpdate.Record = array.NewTableFromRecords(handle.schema, pendingRecords)
				updateChan <- ticking.TickingStatus{Update: *pendingUpdate}
				pendingUpdate = nil
				pendingRecords = nil
				return true, nil
			} else {
				return false, nil
			}
		}

		for reader.Next() {
			resp := reader.LatestAppMetadata()

			if len(resp) > 0 {
				if pendingUpdate != nil {
					updateChan <- ticking.TickingStatus{Err: errors.New("previous update was never completed")}
					return
				}

				meta := resp
				msgWrapper := flatbuf_b.GetRootAsBarrageMessageWrapper(meta, 0)

				payload := make([]byte, msgWrapper.MsgPayloadLength())
				for i := 0; i < msgWrapper.MsgPayloadLength(); i++ {
					payload[i] = byte(msgWrapper.MsgPayload(i))
				}

				if msgWrapper.MsgType() == flatbuf_b.BarrageMessageTypeBarrageUpdateMetadata {
					updateMeta := flatbuf_b.GetRootAsBarrageUpdateMetadata(payload, 0)

					addedRows := make([]byte, updateMeta.AddedRowsLength())
					for i := 0; i < updateMeta.AddedRowsLength(); i++ {
						addedRows[i] = byte(updateMeta.AddedRows(i))
					}

					removedRows := make([]byte, updateMeta.RemovedRowsLength())
					for i := 0; i < updateMeta.RemovedRowsLength(); i++ {
						removedRows[i] = byte(updateMeta.RemovedRows(i))
					}

					addedRowsIncluded := make([]byte, updateMeta.AddedRowsIncludedLength())
					for i := 0; i < updateMeta.AddedRowsIncludedLength(); i++ {
						addedRowsIncluded[i] = byte(updateMeta.AddedRowsIncluded(i))
					}

					shiftData := make([]byte, updateMeta.ShiftDataLength())
					for i := 0; i < updateMeta.ShiftDataLength(); i++ {
						shiftData[i] = byte(updateMeta.ShiftData(i))
					}

					var modifiedRows []ticking.RowSet
					for i := 0; i < updateMeta.ModColumnNodesLength(); i++ {
						modMeta := new(flatbuf_b.BarrageModColumnMetadata)
						updateMeta.ModColumnNodes(modMeta, i)

						modifiedRowSet := make([]byte, modMeta.ModifiedRowsLength())
						for j := 0; j < modMeta.ModifiedRowsLength(); j++ {
							modifiedRowSet[j] = byte(modMeta.ModifiedRows(j))
						}

						modifiedRowSetDe, err := ticking.DeserializeRowSet(modifiedRowSet)
						if err != nil {
							updateChan <- ticking.TickingStatus{Err: err}
							return
						}
						modifiedRows = append(modifiedRows, modifiedRowSetDe)
					}

					removedRowsDec, err := ticking.DeserializeRowSet(removedRows)
					if err != nil {
						updateChan <- ticking.TickingStatus{Err: err}
						return
					}

					startSet, endSet, destSet, err := ticking.DeserializeRowSetShiftData(shiftData)
					if err != nil {
						updateChan <- ticking.TickingStatus{Err: err}
						return
					}

					hasIncludedRows := len(addedRowsIncluded) > 0

					var addedRowsIncSet ticking.RowSet
					if hasIncludedRows {
						addedRowsIncSet, err = ticking.DeserializeRowSet(addedRowsIncluded)
						if err != nil {
							updateChan <- ticking.TickingStatus{Err: err}
							return
						}
					}

					addedRowsDec, err := ticking.DeserializeRowSet(addedRows)
					if err != nil {
						updateChan <- ticking.TickingStatus{Err: err}
						return
					}

					// FIXME: Barrage likes to send Arrow records where not all the columns are the same length.
					// The Go Arrow library has a check against this and panics when it receives such a record.
					// Solutions:
					// - Copy/paste the entirety of the reader code from Apache Arrow so we can remove the check
					// - Make a barrage subscription option that pads records so that all columns are the same length
					//		(preferably, make the ragged edges an opt-in)
					record := reader.Record()
					record.Retain()

					pendingRecords = append(pendingRecords, record)

					pendingUpdate = &ticking.TickingUpdate{
						IsSnapshot: updateMeta.IsSnapshot(),

						AddedRows:   addedRowsDec,
						RemovedRows: removedRowsDec,

						AddedRowsIncluded: addedRowsIncSet,
						HasIncludedRows:   hasIncludedRows,

						ShiftDataStarts: startSet,
						ShiftDataEnds:   endSet,
						ShiftDataDests:  destSet,

						ModifiedRows: modifiedRows,
					}

					_, err = trySubmitUpdate()
					if err != nil {
						updateChan <- ticking.TickingStatus{Err: err}
						return
					}
				} else {
					fmt.Println("TODO: OTHER MESSAGE TYPE")
				}
			} else {
				record := reader.Record()
				record.Retain()

				pendingRecords = append(pendingRecords, record)
				_, err = trySubmitUpdate()
				if err != nil {
					updateChan <- ticking.TickingStatus{Err: err}
				}
			}
		}

		err = reader.Err()
		if err != nil {
			// Cancellation errors are ignored, since that's the usual way to stop a subscription.
			if err, ok := status.FromError(err); !ok || err.Code() != codes.Canceled {
				updateChan <- ticking.TickingStatus{Err: reader.Err()}
			}
			return
		}
	}()

	return tbl, updateChan, err
}
