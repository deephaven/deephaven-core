package client

import (
	"context"
	"errors"
	"fmt"

	apppb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/application"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Wraps gRPC calls for application.proto.
type appStub struct {
	client *Client

	// The stub for application.proto gRPC requests.
	stub apppb2.ApplicationServiceClient

	// Only present when a FetchRepeating call is running or has been canceled.
	savedContext context.Context
	// Only present when doing a FetchRepeating call in the background.
	// Calling this function will cancel the FetchRepeating call.
	cancelFunc context.CancelFunc
	// Only present when doing a FetchRepeating call in the background.
	// listFields must wait for this channel to close, so it knows that the call has stopped.
	cancelAck <-chan struct{}
}

func newAppStub(client *Client) appStub {
	return appStub{client: client, stub: apppb2.NewApplicationServiceClient(client.grpcChannel)}
}

type changeHandler func(update *apppb2.FieldsChangeUpdate)

// listFields wraps the ListFields gRPC request, which streams back a list of created/updated/removed fields.
//
// This function blocks on the first response (which the server always returns synchronously) and immediately
// passes it to the given handler.
//
// A thread that continues reading the listFields request and passing responses to the handler
// will start in the background if FetchRepeating is specified.
//
// Note that the provided context is saved and used to continue reading the stream,
// so any timeouts or cancellations that have been set for it will affect the listFields stream.
//
// The client lock should be held when calling this function.
func (as *appStub) listFields(ctx context.Context, fetchOption FetchOption, handler changeHandler) error {
	ctx, err := as.client.withToken(ctx)
	if err != nil {
		return err
	}

	// Make a copy of the context now, since it shouldn't include the cancellation function,
	// but don't actually save it until we know the loop has started successfully.
	ctxToBeSaved := ctx

	ctx, cancel := context.WithCancel(ctx)

	as.cancelFetchLoop()

	req := apppb2.ListFieldsRequest{}
	fieldStream, err := as.stub.ListFields(ctx, &req)
	if err != nil {
		cancel()
		return err
	}

	resp, err := fieldStream.Recv()
	if err != nil {
		cancel()
		return err
	}

	handler(resp)

	as.cancelFunc = cancel

	if fetchOption == FetchRepeating {
		cancelAck := make(chan struct{})
		as.savedContext = ctxToBeSaved
		as.cancelAck = cancelAck
		go fetchTablesLoop(fieldStream, handler, cancelAck)
		return nil
	} else {
		as.cancelFetchLoop()
		return nil
	}
}

// isFetching returns true if an existing FetchTables loop is running.
// The client lock should be held when calling this function.
func (as *appStub) isFetching() bool {
	return as.cancelFunc != nil
}

// cancelFetchLoop cancels an existing FetchTables loop, if one exists.
// If one exists, this will block until the loop has finished writing any results that are in-progress.
// The client lock should be held when calling this function.
func (as *appStub) cancelFetchLoop() {
	if as.cancelFunc != nil {
		as.cancelFunc()
		as.cancelFunc = nil
	}

	if as.cancelAck != nil {
		<-as.cancelAck
		as.cancelAck = nil
	}
}

// resumeFetchLoop resumes a FetchTables loop that has been canceled,
// starting it with its original context.
// The client lock should be held when calling this function.
func (as *appStub) resumeFetchLoop(handler changeHandler) error {
	if as.isFetching() {
		return errors.New("tried to resume a fetch loop while one was already running")
	}
	if as.savedContext == nil {
		return errors.New("tried to resume a fetch loop but one was never canceled")
	}

	return as.listFields(as.savedContext, FetchRepeating, handler)
}

// Close closes the app stub and frees any associated resources.
// The app stub should not be used after calling this function.
// The client lock should be held when calling this function.
func (as *appStub) Close() {
	as.cancelFetchLoop()
}

// fetchTablesLoop runs a loop that will check for new changes to fields.
// When a new change occurs, it will pass the change to the given handler.
// When the context for the stream gets canceled, the loop will halt
// and close the cancelAck channel to signal that it is finished.
func fetchTablesLoop(stream apppb2.ApplicationService_ListFieldsClient, handler changeHandler, cancelAck chan<- struct{}) {
	for {
		resp, err := stream.Recv()
		if err != nil {
			if status, ok := status.FromError(err); ok && status.Code() == codes.Canceled {
				close(cancelAck)
				break
			}
			fmt.Println("failed to list fields: ", err)
			return
		}

		handler(resp)
	}
}
