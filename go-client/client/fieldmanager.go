package client

import (
	"context"
	"fmt"
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
	fs *fieldStream
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// FetchOption specifies the kind of fetch to be done when using FetchTables.
// See the docs for FetchOnce, FetchRepeating, and FetchTables for more information.
type FetchOption int

const (
	FetchOnce      FetchOption = iota // FetchOnce fetches the list of tables once and then returns.
	FetchRepeating                    // FetchRepeating starts up a background goroutine to continually update the list of tables as changes occur.
)

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

func (fs *fieldStream) FetchOnce() (*apppb2.FieldsChangeUpdate, error) {
	return fs.fieldsClient.Recv()
}

func (fs *fieldStream) FetchRepeating(stream chan respFMExec) {
	go func() {
		for {
			changes, err := fs.fieldsClient.Recv()
			stream <- respFMExec{changes: changes, err: err}

			if err != nil {
				if status, ok := status.FromError(err); ok && status.Code() == codes.Canceled {
					//todo log?
					return
				}
				//todo how to handle err cases
				fmt.Println("failed to list fields: ", err)
			}
		}
	}()
}

func (fs *fieldStream) Stop() {
	fs.cancel()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//todo doc
type fieldManager struct {
	client *Client
	tables map[fieldId]*TableHandle // A map of tables that can be opened using OpenTable
	//stream apppb2.ApplicationService_ListFieldsClient
	fs *fieldStream

	chanClose          chan reqClose
	chanOpenTables     chan reqListOpenableTables
	chanGetTable       chan reqGetTable
	chanFMExec         chan reqFMExec
	chanFetchRepeating chan reqFetchRepeating
}

func newFieldManager(client *Client) fieldManager {
	fm := fieldManager{
		client: client,
		tables: make(map[fieldId]*TableHandle),

		chanClose:          make(chan reqClose),
		chanOpenTables:     make(chan reqListOpenableTables),
		chanGetTable:       make(chan reqGetTable),
		chanFMExec:         make(chan reqFMExec),
		chanFetchRepeating: make(chan reqFetchRepeating),
	}

	go fm.loop()

	return fm
}

// fetchTablesLoop runs a loop that will check for new changes to fields.
// When a new change occurs, it will pass the change to the given handler.
// When the context for the stream gets canceled, the loop will halt
// and close the cancelAck channel to signal that it is finished.
func (fm *fieldManager) loop() {
	chanChanges := make(chan respFMExec)

	for {
		select {
		case <-fm.chanClose:
			//todo log
			return
		case resp := <-chanChanges:
			//todo review error model here...
			if resp.err != nil {
				if status, ok := status.FromError(resp.err); ok && status.Code() == codes.Canceled {
					//todo log?
					//todo ???? what is the right error case?  log and go on?  fatal?  etc.  If cancelled, should Close be called?
					break
				}
				//todo how to handle err case
				fmt.Println("failed to list fields: ", resp.err)
				return
			}

			fm.handleFieldChanges(resp.changes)
		case req := <-fm.chanOpenTables:
			req.out <- fm.listOpenableTables()
		case req := <-fm.chanGetTable:
			tbl, ok := fm.tables[req.id]
			req.out <- respGetTable{table: tbl, ok: ok}
		case req := <-fm.chanFetchRepeating:
			if fm.fs != nil {
				fm.fs.Stop()
			}

			fm.fs = req.fs
			fm.fs.FetchRepeating(chanChanges)
		case req := <-fm.chanFMExec:
			changes, err := req.f()
			msg := respFMExec{changes: changes, err: err}
			chanChanges <- msg
			req.out <- msg
		}
	}
}

//todo clean up
// handleFieldChanges updates the list of fields the client currently knows about
// according to changes made elsewhere (e.g. from fields changed from a script response or from a ListFields request).
func (fm *fieldManager) handleFieldChanges(changes *apppb2.FieldsChangeUpdate) {
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

//todo doc
func (fm *fieldManager) Close() {
	//todo fm.chanClose <- reqClose{}
	if fm.chanClose != nil {
		close(fm.chanClose)
	}

	if fm.fs != nil {
		fm.fs.Stop()
	}
}

//todo doc
func (fm *fieldManager) listOpenableTables() []string {
	var result []string

	for id := range fm.tables {
		if id.appId == "scope" {
			result = append(result, id.fieldName)
		}
	}

	return result
}

//todo doc
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

//todo doc
func (fm *fieldManager) GetTable(id fieldId) (*TableHandle, bool) {
	req := reqGetTable{id: id, out: make(chan respGetTable)}
	fm.chanGetTable <- req
	rst := <-req.out
	return rst.table, rst.ok
}

//todo doc
func (fm *fieldManager) ExecAndUpdate(f funcFMExec) error {
	req := reqFMExec{f: f, out: make(chan respFMExec)}
	fm.chanFMExec <- req
	resp := <-req.out
	return resp.err
}

func (fm *fieldManager) FetchTables(ctx context.Context, appServiceClient apppb2.ApplicationServiceClient, opt FetchOption) error {
	fs, err := newFieldStream(ctx, appServiceClient)

	if err != nil {
		return err
	}

	switch opt {
	case FetchOnce:
		return fm.ExecAndUpdate(fs.FetchOnce)
	case FetchRepeating:
		fm.chanFetchRepeating <- reqFetchRepeating{fs: fs}
	}

	return nil
}
