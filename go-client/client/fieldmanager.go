package client

import (
	"context"

	apppb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/application"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A fieldId is a unique identifier for a field on the server,
// where a "field" could be e.g. a table or a plot.
type fieldId struct {
	appId     string // appId is the application scope for the field. For the global scope this is "scope".
	fieldName string // fieldName is the name of the field.
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// reqClose is a field manager request used to stop an executor.
// The returned channel will close to signal that the executor is finished.
type reqClose struct{ out chan<- struct{} }

// respListOpenableTables is a field manager response containing a list of table names that can be opened using GetTable.
type respListOpenableTables []string

// reqListOpenableTables is a field manager request used to fetch a list of table names that can be opened using GetTable.
type reqListOpenableTables struct{ out chan respListOpenableTables }

// respGetTable is a field manager response containing a newly-opened table handle from GetTable.
type respGetTable struct {
	table *TableHandle
	ok    bool
}

// reqGetTable is a field manager request used to open a new table handle given a table name.
type reqGetTable struct {
	id  fieldId
	out chan respGetTable
}

// funcFMExec is a function that can be called by the executor and returns some list of field changes.
type funcFMExec func() (*apppb2.FieldsChangeUpdate, error)

// respFMExec is the result of calling a funcFMExec.
type respFMExec struct {
	changes *apppb2.FieldsChangeUpdate
	err     error
}

// reqFMExec is a field manager request that makes the executor call a function and handle its result.
type reqFMExec struct {
	f   funcFMExec
	out chan respFMExec // The function's return value is sent over this channel.
}

// reqFetchRepeating is a field manager request that starts a new FetchRepeating request.
type reqFetchRepeating struct {
	fs        *fieldStream
	chanError chan<- error
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
// If neither FetchOnce or FetchRepeating have already been called on this stream, this will return immediately.
func (fs *fieldStream) FetchOnce() (*apppb2.FieldsChangeUpdate, error) {
	return fs.fieldsClient.Recv()
}

// FetchRepeating starts a goroutine that will forward responses from the ListFields request
// to the given channel. The goroutine will loop infinitely until Close() is called
// or an error occurs.
// This function will close the stream channel when it terminates.
func (fs *fieldStream) FetchRepeating(stream chan<- respFMExec) {
	go func() {
		for {
			changes, err := fs.fieldsClient.Recv()
			stream <- respFMExec{changes: changes, err: err}

			if err != nil {
				// The error will be handled by the fieldManagerExecutor.
				fs.Close()
				close(stream)
				return
			}
		}
	}()
}

// Close closes the field stream and stops any existing FetchRepeating requests.
func (fs *fieldStream) Close() {
	if fs.cancel != nil {
		fs.cancel()
		fs.cancel = nil
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// isCanceledError returns true if the error is a gRPC Canceled error.
func isCanceledError(err error) bool {
	status, ok := status.FromError(err)
	return ok && status.Code() == codes.Canceled
}

// fieldManagerExecutor handles requests from a fieldManager and performs the appropriate actions.
// It controls the state of FetchTablesRepeating calls and holds the list of tables that can be opened.
type fieldManagerExecutor struct {
	client *Client
	tables map[fieldId]*TableHandle // A map of tables that can be opened using OpenTable
	fs     *fieldStream             // The current stream for a FetchRepeating request. This is nil if no FetchRepeating request is running.

	chanClose               <-chan reqClose
	chanOpenTables          <-chan reqListOpenableTables
	chanGetTable            <-chan reqGetTable
	chanFMExec              <-chan reqFMExec
	chanFetchRepeating      <-chan reqFetchRepeating
	chanFetchRepeatingError chan<- error // Errors coming from FetchRepeating requests will be sent to this channel.

	// Receives changes from a running FetchRepeating request.
	// This is nil if no FetchRepeating request is running.
	chanChanges <-chan respFMExec
}

// loop starts up an executor loop, which will handle requests
// from the executor's channels and perform the appropriate actions.
// It will run forever until chanClose is closed.
func (fme *fieldManagerExecutor) loop() {
	for {
		select {
		case req := <-fme.chanClose:
			fme.cancelFieldStream()
			req.out <- struct{}{}
			return
		case req := <-fme.chanOpenTables:
			req.out <- fme.listOpenableTables()
		case req := <-fme.chanGetTable:
			tbl, ok := fme.tables[req.id]
			req.out <- respGetTable{table: tbl, ok: ok}
		case req := <-fme.chanFMExec:
			changes, err := req.f()
			msg := respFMExec{changes: changes, err: err}
			// The ExecAndUpdate call will handle the error for us.
			req.out <- msg
			if err == nil {
				fme.handleFieldChanges(changes)
			}
		case req := <-fme.chanFetchRepeating:
			fme.cancelFieldStream()

			fme.fs = req.fs
			fme.chanFetchRepeatingError = req.chanError

			chanChanges := make(chan respFMExec)
			fme.fs.FetchRepeating(chanChanges)
			fme.chanChanges = chanChanges

			// The first response is always synchronous,
			// so we handle it immediately.
			resp := <-fme.chanChanges
			fme.handleFetchResponse(resp)
		case resp, ok := <-fme.chanChanges:
			if ok {
				fme.handleFetchResponse(resp)
			} else {
				fme.chanChanges = nil
			}
		}
	}
}

// handleFetchResponse handles a response from a FetchRepeating request.
// It will update the list of tables or forward an error to the request's associated error channel as appropriate.
func (fme *fieldManagerExecutor) handleFetchResponse(resp respFMExec) {
	// Canceled errors are ignored,
	// since they happen normally when stopping a request.
	if isCanceledError(resp.err) {
		return
	}
	if resp.err != nil {
		fme.chanFetchRepeatingError <- resp.err
		fme.cancelFieldStream()
		return
	}
	fme.handleFieldChanges(resp.changes)
}

// cancelFieldStream cancels the executor's existing FetchRepeating request (if one is running).
// It then handles any remaining responses from the request.
// Finally, it closes the request's associated error channel.
func (fme *fieldManagerExecutor) cancelFieldStream() {
	if fme.fs != nil {
		fme.fs.Close()
		fme.fs = nil
	} else {
		return
	}

	if fme.chanChanges != nil {
		for resp := range fme.chanChanges {
			fme.handleFetchResponse(resp)
		}
		fme.chanChanges = nil
	}

	if fme.chanFetchRepeatingError != nil {
		close(fme.chanFetchRepeatingError)
		fme.chanFetchRepeatingError = nil
	}
}

// handleFieldChanges updates the list of fields the client currently knows about
// according to changes made elsewhere (e.g. from fields changed from a script response or from a ListFields request).
func (fm *fieldManagerExecutor) handleFieldChanges(changes *apppb2.FieldsChangeUpdate) {
	for _, created := range changes.Created {
		if created.TypedTicket.Type == "Table" {
			fieldId := fieldId{appId: created.ApplicationId, fieldName: created.FieldName}
			fm.tables[fieldId] = newTableHandle(fm.client, created.TypedTicket.Ticket, nil, 0, false)
		}
	}

	for _, updated := range changes.Updated {
		if updated.TypedTicket.Type == "Table" {
			fieldId := fieldId{appId: updated.ApplicationId, fieldName: updated.FieldName}
			fm.tables[fieldId] = newTableHandle(fm.client, updated.TypedTicket.Ticket, nil, 0, false)
		}
	}

	for _, removed := range changes.Removed {
		if removed.TypedTicket.Type == "Table" {
			fieldId := fieldId{appId: removed.ApplicationId, fieldName: removed.FieldName}
			delete(fm.tables, fieldId)
		}
	}
}

// listOpenableTables returns a list of the (global) tables that can be opened with OpenTable.
func (fm *fieldManagerExecutor) listOpenableTables() []string {
	var result []string

	for id := range fm.tables {
		if id.appId == "scope" {
			result = append(result, id.fieldName)
		}
	}

	return result
}

// A fieldManager is a wrapper for a fieldManagerExecutor.
// The channels send requests to the executor, and the executor does the actual work.
// This is essentially just a way of offering goroutine-safe methods without explicit locking.
type fieldManager struct {
	chanClose          chan<- reqClose // Closing this channel will stop the executor.
	chanOpenTables     chan<- reqListOpenableTables
	chanGetTable       chan<- reqGetTable
	chanFMExec         chan<- reqFMExec
	chanFetchRepeating chan<- reqFetchRepeating
}

// ListOpenableTables returns a list of the (global) tables that can be opened with OpenTable.
// Tables that are created by other clients or in the web UI are not listed here automatically.
// Tables that are created in scripts run by this client, however, are immediately available,
// and will be added to/removed from the list as soon as the script finishes.
// FetchTables can be used to update the list to reflect what tables are currently available
// from other clients or the web UI.
// Calling FetchTables with a FetchRepeating argument will continually update this list in the background.
func (fm *fieldManager) ListOpenableTables() []string {
	c := make(chan respListOpenableTables)
	fm.chanOpenTables <- reqListOpenableTables{out: c}
	return <-c
}

// getTable returns the table with the given ID, if it is present in the local list.
// If the table is not present, ok is returned as false.
// The returned table handle is not exported, so TableHandle.Release should not be called on it.
// Instead, use OpenTable or fetchTable to get an exported TableHandle that can be returned to the user.
// See ListOpenableTables for details on how the local list can get out of sync.
func (fm *fieldManager) getTable(id fieldId) (table *TableHandle, ok bool) {
	req := reqGetTable{id: id, out: make(chan respGetTable)}
	fm.chanGetTable <- req
	rst := <-req.out
	return rst.table, rst.ok
}

// ExecAndUpdate passes the given function to the executor,
// which will call the function and add/remove tables to its list according to the response.
func (fm *fieldManager) ExecAndUpdate(f funcFMExec) error {
	req := reqFMExec{f: f, out: make(chan respFMExec)}
	fm.chanFMExec <- req
	resp := <-req.out
	return resp.err
}

// FetchTablesOnce fetches the list of tables from the server.
// This allows the client to see the list of named global tables on the server,
// and thus allows the client to open them using OpenTable.
// Tables created in scripts run by the current client are immediately visible and do not require a FetchTables call.
// If you need to update the list of tables frequently, consider using FetchTablesRepeating instead.
func (fm *fieldManager) FetchTablesOnce(ctx context.Context, appServiceClient apppb2.ApplicationServiceClient) error {
	fs, err := newFieldStream(ctx, appServiceClient)

	if err != nil {
		return err
	}

	result := fm.ExecAndUpdate(fs.FetchOnce)
	fs.Close()
	return result
}

// FetchTablesRepeating starts up a goroutine that fetches the list of tables from the server continuously.
// This allows the client to see the list of named global tables on the server,
// and thus allows the client to open them using OpenTable.
// Tables created in scripts run by the current client are immediately visible and do not require a FetchTables call.
// If you only need to update the list of tables once, consider using FetchTablesOnce instead.
func (fm *fieldManager) FetchTablesRepeating(ctx context.Context, appServiceClient apppb2.ApplicationServiceClient) <-chan error {
	// This channel gets a buffer size of 1 so that a goroutine can put an error on the channel
	// and then immediately terminate, regardless of whether or not a receiver is actually listening.
	chanError := make(chan error, 1)

	fs, err := newFieldStream(ctx, appServiceClient)

	if err != nil {
		chanError <- err
		return chanError
	}

	fm.chanFetchRepeating <- reqFetchRepeating{fs: fs, chanError: chanError}
	return chanError
}

// Close closes the fieldManager and frees any associated resources.
// It will also stop the associated executor and any running FetchTablesRepeating requests.
// Once Close has been called, no other methods should be used on the fieldManager (except Close again, which will do nothing).
// The client lock should be held while calling this function.
func (fm *fieldManager) Close() {
	if fm.chanClose != nil {
		// This waits for the executor to stop.
		closeConfirm := make(chan struct{})
		fm.chanClose <- reqClose{out: closeConfirm}
		close(fm.chanClose)
		<-closeConfirm

		fm.chanClose = nil
		fm.chanOpenTables = nil
		fm.chanGetTable = nil
		fm.chanFMExec = nil
		fm.chanFetchRepeating = nil
	}
}

// newFieldManager returns a field manager for the given client.
// The returned field manager will have an associated executor which runs in a separate goroutine.
func newFieldManager(client *Client) fieldManager {
	chanClose := make(chan reqClose)
	chanOpenTables := make(chan reqListOpenableTables)
	chanGetTable := make(chan reqGetTable)
	chanFMExec := make(chan reqFMExec)
	chanFetchRepeating := make(chan reqFetchRepeating)

	fm := fieldManager{
		chanClose:          chanClose,
		chanOpenTables:     chanOpenTables,
		chanGetTable:       chanGetTable,
		chanFMExec:         chanFMExec,
		chanFetchRepeating: chanFetchRepeating,
	}

	fme := fieldManagerExecutor{
		client: client,
		tables: make(map[fieldId]*TableHandle),

		chanClose:          chanClose,
		chanOpenTables:     chanOpenTables,
		chanGetTable:       chanGetTable,
		chanFMExec:         chanFMExec,
		chanFetchRepeating: chanFetchRepeating,
	}

	go fme.loop()

	return fm
}
