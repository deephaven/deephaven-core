package client

import (
	"context"
	"fmt"

	apppb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/application"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Wraps gRPC calls for application.proto.
type appStub struct {
	client *Client

	stub apppb2.ApplicationServiceClient

	// Only present when doing a FetchRepeating call in the background.
	// Calling this function will cancel the FetchRepeating call.
	cancelFunc context.CancelFunc
}

func newAppStub(client *Client) appStub {
	return appStub{client: client, stub: apppb2.NewApplicationServiceClient(client.grpcChannel)}
}

type changeHandler func(update *apppb2.FieldsChangeUpdate)

// Note that the provided context is saved and used to continue reading the stream,
// so any timeouts or cancellations that have been set for it will affect the listFields stream.
// A thread that continues reading the listFields request and updating the client's tables will start in the background
// if FetchRepeating is specified.
func (as *appStub) listFields(ctx context.Context, fetchOption FetchOption, handler changeHandler) error {
	ctx, err := as.client.withToken(ctx)
	if err != nil {
		return err
	}

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
		go fetchTablesLoop(fieldStream, handler)

		return nil
	} else {
		as.cancelFetchLoop()

		return nil
	}
}

func (as *appStub) isFetching() bool {
	return as.cancelFunc != nil
}

func (as *appStub) cancelFetchLoop() {
	if as.cancelFunc != nil {
		as.cancelFunc()
		as.cancelFunc = nil
	}
}

func (as *appStub) Close() {
	as.cancelFetchLoop()
}

func fetchTablesLoop(stream apppb2.ApplicationService_ListFieldsClient, handler changeHandler) {
	for {
		resp, err := stream.Recv()
		if err != nil {
			if status, ok := status.FromError(err); ok && status.Code() == codes.Canceled {
				break
			}
			fmt.Println("failed to list fields: ", err)
			return
		}

		handler(resp)
	}
}
