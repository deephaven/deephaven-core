package client

import (
	"context"

	apppb2 "github.com/deephaven/deephaven-core/go/client/internal/proto/application"
)

// A fieldId is a unique identifier for a field on the server,
// where a "field" could be e.g. a table or a plot.
type fieldId struct {
	appId     string // appId is the application scope for the field. For the global scope this is "scope".
	fieldName string // fieldName is the name of the field.
}

// fieldStream wraps a ListFields request, allowing it to be easily closed
// or for its responses to be forwarded to a channel.
// It should be closed using Close() when it is no longer needed.
type fieldStream struct {
	fieldsClient apppb2.ApplicationService_ListFieldsClient
	cancel       context.CancelFunc
}

// newFieldStream starts a new ListFields gRPC request.
// The gRPC request can then be controlled using the other fieldStream methods.
func newFieldStream(ctx context.Context, appServiceClient apppb2.ApplicationServiceClient) (*fieldStream, error) {
	ctx, cancel := context.WithCancel(ctx)

	req := apppb2.ListFieldsRequest{}
	fieldsClient, err := appServiceClient.ListFields(ctx, &req)

	if err != nil {
		cancel()
		return nil, err
	}

	fs := &fieldStream{fieldsClient: fieldsClient, cancel: cancel}
	return fs, nil
}

// FetchOnce returns the next set of changes from the ListFields request.
// If FetchOnce has not already been called on this stream, this will return immediately.
func (fs *fieldStream) FetchOnce() (*apppb2.FieldsChangeUpdate, error) {
	return fs.fieldsClient.Recv()
}

// Close closes the field stream.
// This method is NOT goroutine-safe.
func (fs *fieldStream) Close() {
	if fs.cancel != nil {
		fs.cancel()
		fs.cancel = nil
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// getFields returns a list of all fields currently present on the server.
func (client *Client) getFields(ctx context.Context) ([]*apppb2.FieldInfo, error) {
	ctx, err := client.withToken(ctx)
	if err != nil {
		return nil, err
	}

	fs, err := newFieldStream(ctx, client.appServiceClient)
	if err != nil {
		return nil, err
	}
	// It's okay to call Close here since the fs is owned exclusively by this goroutine.
	defer fs.Close()

	changes, err := fs.FetchOnce()
	if err != nil {
		return nil, err
	}

	// This is the first response, so only the Created part matters
	return changes.Created, nil
}

// getTable returns the table with the given ID, if it is present on the server.
// If the table is not present, the returned table pointer will be nil.
// The returned table handle is not exported, so TableHandle.Release should not be called on it.
// Instead, use OpenTable or fetchTable to get an exported TableHandle that can be returned to the user.
func (client *Client) getTable(ctx context.Context, id fieldId) (*TableHandle, error) {
	fields, err := client.getFields(ctx)
	if err != nil {
		return nil, err
	}

	for _, f := range fields {
		if f.ApplicationId == id.appId && f.FieldName == id.fieldName && f.TypedTicket.Type == "Table" {
			return newBorrowedTableHandle(client, f.TypedTicket.Ticket, nil, 0, false), nil
		}
	}

	return nil, nil
}

// ListOpenableTables returns a list of the (global) tables that can be opened with OpenTable.
// Tables bound to variables by other clients or the web UI will show up in this list,
// though it is not guaranteed how long it will take for new tables to appear.
func (client *Client) ListOpenableTables(ctx context.Context) ([]string, error) {
	fields, err := client.getFields(ctx)
	if err != nil {
		return nil, err
	}

	var tables []string
	for _, id := range fields {
		if id.ApplicationId == "scope" && id.TypedTicket.Type == "Table" {
			tables = append(tables, id.FieldName)
		}
	}
	return tables, nil
}
