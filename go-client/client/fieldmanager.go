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

//todo doc all

type reqClose struct{}

type respListOpenableTables []string
type reqListOpenableTables struct{ out chan respListOpenableTables }

type respGetTable struct {
	table *TableHandle
	ok    bool
}
type reqGetTable struct {
	id  fieldId
	out chan respGetTable
}

type funcFMExec func() (*apppb2.FieldsChangeUpdate, error)
type respFMExec struct {
	changes *apppb2.FieldsChangeUpdate
	err     error
}
type reqFMExec struct {
	f   funcFMExec
	out chan respFMExec
}

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
// to the given channel. The goroutine will loop infinitely until Close() is called.
func (fs *fieldStream) FetchRepeating(stream chan<- respFMExec) {
	go func() {
		for {
			changes, err := fs.fieldsClient.Recv()
			stream <- respFMExec{changes: changes, err: err}

			if err != nil {
				// The error will be handled by the fieldManagerExecutor.
				fs.Close()
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
}

// loop starts up an executor loop, which will handle requests
// from the executor's channels and perform the appropriate actions.
// It will run forever until chanClose is closed.
func (fme *fieldManagerExecutor) loop() {
	// This channel is used to send/receive responses from FetchRepeating requests.
	chanChanges := make(chan respFMExec)

	defer func() {
		if fme.fs != nil {
			fme.fs.Close()
			fme.fs = nil
		}

		if fme.chanFetchRepeatingError != nil {
			close(fme.chanFetchRepeatingError)
			fme.chanFetchRepeatingError = nil
		}
	}()

	for {
		select {
		case <-fme.chanClose:
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
			if fme.fs != nil {
				fme.fs.Close()
				fme.fs = nil
			}

			if fme.chanFetchRepeatingError != nil {
				close(fme.chanFetchRepeatingError)
				fme.chanFetchRepeatingError = nil
			}

			fme.fs = req.fs
			fme.chanFetchRepeatingError = req.chanError
			// TODO: The first response is always synchronous,
			// and it would be nice to handle it immediately.
			// However, there might be existing responses from the previous fetch,
			// so we can't just immediately read from the channel and take the first result.
			fme.fs.FetchRepeating(chanChanges)
		case resp := <-chanChanges:
			// Canceled errors are ignored,
			// since they happen normally when stopping a request.
			if isCanceledError(resp.err) {
				break
			}
			if resp.err != nil {
				fme.chanFetchRepeatingError <- resp.err
				close(fme.chanFetchRepeatingError)
				fme.chanFetchRepeatingError = nil
				fme.fs.Close()
				break
			}
			fme.handleFieldChanges(resp.changes)
		}
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

// fieldManager is an interface to fieldManagerExecutor.
// The channels communicate with an executor, and the executor does the actual work.
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
func (fm *fieldManager) Close() {
	if fm.chanClose != nil {
		close(fm.chanClose)

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
