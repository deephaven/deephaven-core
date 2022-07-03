// This package allows you to interface with a Deephaven server over a network connection using Go.
// It can upload, manipulate, and download tables, among other features.
// To get started, use client.NewClient to connect to the server. The Client can then be used to perform operations.
// See the provided examples in the examples/ folder or the individual code documentation for more.
// The Go API uses Records from the Apache Arrow package as tables,
// All methods are thread-safe unless otherwise specified.
package client

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"

	applicationpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/application"
	consolepb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/console"
	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// ErrClosedClient is returned as an error when trying to perform a network operation on a client that has been closed.
var ErrClosedClient = errors.New("client is closed")

// A fieldId is a unique identifier for a field on the server,
// where a "field" could be e.g. a table or a plot.
type fieldId struct {
	appId     string // appId is the application scope for the field. For the global scope this is "scope".
	fieldName string // fieldName is the name of the field.
}

//todo doc
//todo move to another file?
type ticketFactory struct {
	id int32
}

// newTicketNum returns a new ticket number that has not been used before.
func (tf *ticketFactory) nextId() int32 {
	nextTicket := atomic.AddInt32(&tf.id, 1)

	if nextTicket <= 0 {
		// If you ever see this panic... what are you doing?
		panic("out of tickets")
	}

	return nextTicket
}

// Maintains a connection to a Deephaven server.
// It can be used to run scripts, create new tables, execute queries, etc.
// Check the various methods of Client to learn more.
type Client struct {
	// Guards client-wide state. This means specifically:
	// - Is a FetchTables request running?
	// - Is the client closed?
	// Other functionality does not need a lock, since the gRPC interface is already thread-safe,
	// the tables array has its own lock, and the session token also has its own lock.
	lock sync.Mutex

	grpcChannel *grpc.ClientConn

	sessionStub
	consoleStub
	flightStub
	tableStub
	appStub
	inputTableStub

	ticketMan ticketFactory

	tablesLock sync.Mutex               // Guards the tables map.
	tables     map[fieldId]*TableHandle // A map of tables that can be opened using OpenTable
}

// NewClient starts a connection to a Deephaven server.
//
// scriptLanguage can be either "python" or "groovy", and must match the language used on the server. Python is the default.
//
// The client should be closed using Close() after it is done being used.
//
// Keepalive messages are sent automatically by the client to the server at a regular interval (~30 seconds)
// so that the connection remains open. The provided context is saved and used to send keepalive messages.
func NewClient(ctx context.Context, host string, port string, scriptLanguage string) (*Client, error) {
	grpcChannel, err := grpc.Dial(host+":"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	client := &Client{grpcChannel: grpcChannel, tables: make(map[fieldId]*TableHandle)}

	client.sessionStub, err = newSessionStub(ctx, client)
	if err != nil {
		client.Close()
		return nil, err
	}

	client.consoleStub, err = newConsoleStub(ctx, client, scriptLanguage)
	if err != nil {
		client.Close()
		return nil, err
	}

	client.flightStub, err = newFlightStub(client, host, port)
	if err != nil {
		client.Close()
		return nil, err
	}

	client.tableStub = newTableStub(client)

	client.inputTableStub = newInputTableStub(client)

	client.appStub = newAppStub(client)

	return client, nil
}

// Closed checks if the client is closed, i.e. it can no longer perform operations on the server.
func (client *Client) Closed() bool {
	return client.grpcChannel == nil
}

// FetchOption specifies the kind of fetch to be done when using FetchTables.
// See the docs for FetchOnce, FetchRepeating, and FetchTables for more information.
type FetchOption int

const (
	FetchOnce      FetchOption = iota // FetchOnce fetches the list of tables once and then returns.
	FetchRepeating                    // FetchRepeating starts up a background goroutine to continually update the list of tables as changes occur.
)

// FetchTables fetches the list of tables from the server.
// This allows the client to see the list of named global tables on the server,
// and thus allows it to open them using OpenTable.
// Tables created in scripts run by the current client are immediately visible and do not require a FetchTables call.
func (client *Client) FetchTables(ctx context.Context, opt FetchOption) error {
	// Guards the listFields state.
	client.lock.Lock()
	defer client.lock.Unlock()

	return client.appStub.listFields(ctx, opt, func(update *applicationpb2.FieldsChangeUpdate) {
		client.handleFieldChanges(update)
	})
}

// resumeFetchTables is like FetchTables, but restarts a loop that was stopped earlier.
// The client lock must be held while calling this function.
func (client *Client) resumeFetchTables() error {
	return client.appStub.resumeFetchLoop(func(update *applicationpb2.FieldsChangeUpdate) {
		client.handleFieldChanges(update)
	})
}

// resumeFetchTablesWhileLocked is like fetchTablesWhileLocked, but assumes

// ListOpenableTables returns a list of the (global) tables that can be opened with OpenTable.
// Tables that are created by other clients or in the web UI are not listed here automatically.
// Tables that are created in scripts run by this client, however, are immediately available,
// and will be added to/removed from the list as soon as the script finishes.
// FetchTables can be used to update the list to reflect what tables are currently available
// from other clients or the web UI.
// Calling FetchTables with a FetchRepeating argument will continually update this list in the background.
func (client *Client) ListOpenableTables() []string {
	client.tablesLock.Lock()
	defer client.tablesLock.Unlock()

	var result []string
	for id := range client.tables {
		if id.appId == "scope" {
			result = append(result, id.fieldName)
		}
	}
	return result
}

// newTicket returns a new ticket that this client has not used before.
func (client *Client) newTicket() ticketpb2.Ticket {
	id := client.ticketMan.nextId()
	return client.makeTicket(id)
}

// makeTicket turns a ticket ID into a ticket.
func (client *Client) makeTicket(id int32) ticketpb2.Ticket {
	bytes := []byte{'e', byte(id), byte(id >> 8), byte(id >> 16), byte(id >> 24)}

	return ticketpb2.Ticket{Ticket: bytes}
}

// ExecSerial executes several table operations on the server and returns the resulting tables.
//
// This function makes a request for each table operation.
// Consider using ExecBatch to batch all of the table operations into a single request,
// which can be more efficient.
//
// If this function completes successfully,
// the number of tables returned will always match the number of query nodes passed.
// The first table in the returned list corresponds to the first node argument,
// the second table in the returned list corresponds to the second node argument,
// etc.
//
// This may return a QueryError if the query is invalid.
func (client *Client) ExecSerial(ctx context.Context, nodes ...QueryNode) ([]*TableHandle, error) {
	return execSerial(ctx, client, nodes)
}

// ExecBatch executes a batched query on the server and returns the resulting tables.
//
// All of the operations in the query will be performed in a single request,
// so ExecBatch is usually more efficient than ExecSerial.
//
// If this function completes successfully,
// the number of tables returned will always match the number of query nodes passed.
// The first table in the returned list corresponds to the first node argument,
// the second table in the returned list corresponds to the second node argument,
// etc.
//
// This may return a QueryError if the query is invalid.
func (client *Client) ExecBatch(ctx context.Context, nodes ...QueryNode) ([]*TableHandle, error) {
	return execBatch(client, ctx, nodes)
}

// Close closes the connection to the server and frees any associated resources.
// Once this method is called, the client and any TableHandles from it cannot be used.
func (client *Client) Close() error {
	client.lock.Lock()
	defer client.lock.Unlock()

	client.sessionStub.Close()
	client.appStub.Close()
	if client.grpcChannel != nil {
		client.grpcChannel.Close()
		client.grpcChannel = nil
	}
	// This is logged because most of the time this method is used with defer,
	// which will discard the error value.
	err := client.flightStub.Close()
	if err != nil {
		log.Println("unable to close client:", err.Error())
	}
	return err
}

// withToken attaches the current session token to a context as metadata.
func (client *Client) withToken(ctx context.Context) (context.Context, error) {
	tok, err := client.getToken()
	if err != nil {
		return nil, err
	}
	return metadata.NewOutgoingContext(ctx, metadata.Pairs("deephaven_session_id", string(tok))), nil
}

// RunScript executes a script on the deephaven server.
// The script language depends on the scriptLanguage argument passed when creating the client.
func (client *Client) RunScript(context context.Context, script string) error {
	// This has to shadow the consoleStub method in order to handle the listfields loop

	// This makes sure no other FetchTables calls start in the middle,
	// and also protects the client.tables array.
	client.lock.Lock()
	defer client.lock.Unlock()

	// We have to cancel the loop while a script runs, because otherwise
	// we will get duplicate responses for the same new tables.
	// (We will get a response from RunScript and a response from FetchTables).
	// The fetch loop gets restarted once the RunScript is done.
	restartLoop := client.appStub.isFetching()
	client.appStub.cancelFetchLoop()

	if restartLoop {
		// Clear out the table list to avoid any duplicate entries.
		// It is okay to access client.tables without a lock, because we have already
		// cancelled the fetch loop and acquired a client lock, so there are no concurrent accesses.
		client.tables = make(map[fieldId]*TableHandle)
	}

	err := client.consoleStub.RunScript(context, script)
	if err != nil {
		if restartLoop {
			// If we were fetching tables before we called RunScript,
			// we should try to make sure the loop is restored.
			// No error handling here since we're already handling another error...
			client.resumeFetchTables()
		}
		return err
	}

	if restartLoop {
		// If we were fetching tables before we called RunScript,
		// we should try to make sure the loop is restored.
		err = client.resumeFetchTables()
		if err != nil {
			return err
		}
	}

	return nil
}

// handleScriptChanges updates the list of fields the client currently knows about
// according to changes made by a script.
func (client *Client) handleScriptChanges(resp *consolepb2.ExecuteCommandResponse) {
	client.handleFieldChanges(resp.Changes)
}

// handleFieldChanges updates the list of fields the client currently knows about
// according to changes made elsewhere (e.g. from fields changed from a script response or from a ListFields request).
func (client *Client) handleFieldChanges(resp *applicationpb2.FieldsChangeUpdate) {
	client.tablesLock.Lock()
	defer client.tablesLock.Unlock()

	for _, created := range resp.Created {
		if created.TypedTicket.Type == "Table" {
			fieldId := fieldId{appId: created.ApplicationId, fieldName: created.FieldName}
			client.tables[fieldId] = newTableHandle(client, created.TypedTicket.Ticket, nil, 0, false)
		}
	}

	for _, updated := range resp.Updated {
		if updated.TypedTicket.Type == "Table" {
			fieldId := fieldId{appId: updated.ApplicationId, fieldName: updated.FieldName}
			client.tables[fieldId] = newTableHandle(client, updated.TypedTicket.Ticket, nil, 0, false)
		}
	}

	for _, removed := range resp.Removed {
		if removed.TypedTicket.Type == "Table" {
			fieldId := fieldId{appId: removed.ApplicationId, fieldName: removed.FieldName}
			delete(client.tables, fieldId)
		}
	}
}
