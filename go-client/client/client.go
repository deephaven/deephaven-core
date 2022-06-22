// This package allows you to interface with a Deephaven server over a network connection using Go.
// It can upload, manipulate, and download tables, among other features.
// First, use client.NewClient to connect to the server, then the Client can be used to perform operations.
// See the provided examples in the examples/ folder or the individual code documentation for more.
// All methods are thread-safe unless otherwise specified.
package client

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	applicationpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/application"
	consolepb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/console"
	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Returned as an error when trying to perform a network operation on a client that has been closed.
var ErrClosedClient = errors.New("client is closed")

type fieldId struct {
	appId     string
	fieldName string
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

	// A simple counter that increments every time a new ticket is needed.
	// Must be accessed atomically.
	nextTicket int32

	tablesLock sync.Mutex
	tables     map[fieldId]*TableHandle
}

// Starts a connection to a Deephaven server.
//
// scriptLanguage can be either "python" or "groovy", and must match the language used on the server. Python is the default.
//
// The client should be closed using Close() after it is done being used.
//
// Note that the provided context is saved and used to send keepalive messages.
// Keepalive messages are sent automatically by the client at a regular interval (~30 seconds) so that the connection can continue.
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

	client.tableStub, err = newTableStub(client)
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

	client.inputTableStub = newInputTableStub(client)

	client.appStub = newAppStub(client)

	return client, nil
}

// Checks if the client is closed, i.e. it can no longer perform operations on the server.
func (client *Client) Closed() bool {
	return client.grpcChannel == nil
}

// Specifies the kind of fetch to be done when using FetchTables.
// See the docs for FetchOnce, FetchRepeating, and FetchTables for more information.
type FetchOption int

const (
	FetchOnce      FetchOption = iota // Fetches the list of tables once and then returns.
	FetchRepeating                    // Starts up a background goroutine to continually update the list of tables as changes occur.
)

// Fetches the list of tables from the server.
// This allows the client to see the list of named global tables on the server,
// and thus allows it to open them using OpenTables.
// Tables created in scripts run by the current client are immediately visible and do not require a FetchTables call.
func (client *Client) FetchTables(ctx context.Context, opt FetchOption) error {
	// Guards the listFields state.
	client.lock.Lock()
	defer client.lock.Unlock()

	return client.fetchTablesWhileLocked(ctx, opt)
}

// Like FetchTables, but assumes the client lock is already held.
func (client *Client) fetchTablesWhileLocked(ctx context.Context, opt FetchOption) error {
	return client.listFields(ctx, opt, func(update *applicationpb2.FieldsChangeUpdate) {
		client.handleFieldChanges(update)
	})
}

// Returns a list of the (global) tables that can be opened with OpenTable.
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

// Returns a new ticket number that this client has not used before.
// This function is thread-safe.
func (client *Client) newTicketNum() int32 {
	nextTicket := atomic.AddInt32(&client.nextTicket, 1)
	if nextTicket <= 0 {
		// If you ever see this panic... what are you doing?
		panic("out of tickets")
	}

	return nextTicket
}

// Returns a new ticket that this client has not used before.
// This function is thread-safe.
func (client *Client) newTicket() ticketpb2.Ticket {
	id := client.newTicketNum()

	return client.makeTicket(id)
}

// Turns a  ticket ID into a ticket.
// This function is thread-safe.
func (client *Client) makeTicket(id int32) ticketpb2.Ticket {
	bytes := []byte{'e', byte(id), byte(id >> 8), byte(id >> 16), byte(id >> 24)}

	return ticketpb2.Ticket{Ticket: bytes}
}

// Executes a query on the server and returns the resulting tables.
//
// If this function completes successfully,
// the number of tables returned will always match the number of query nodes passed.
// The first table in the returned list corresponds to the first node argument,
// the second table in the returned list corresponds to the second node argument,
// etc.
//
// This may return a QueryError if the query is invalid.
func (client *Client) ExecQuery(ctx context.Context, nodes ...QueryNode) ([]*TableHandle, error) {
	return execQuery(client, ctx, nodes)
}

// Closes the connection to the server and frees any associated resources.
// Once this method is called, the client and any TableHandles from it cannot be used.
func (client *Client) Close() {
	client.lock.Lock()
	defer client.lock.Unlock()

	client.sessionStub.Close()
	client.appStub.Close()
	if client.grpcChannel != nil {
		client.grpcChannel.Close()
		client.grpcChannel = nil
	}
}

// This is thread-safe
func (client *Client) withToken(ctx context.Context) (context.Context, error) {
	tok, err := client.getToken()
	if err != nil {
		return nil, err
	}
	return metadata.NewOutgoingContext(ctx, metadata.Pairs("deephaven_session_id", string(tok))), nil
}

// Executes a script on the deephaven server.
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
			client.fetchTablesWhileLocked(context, FetchRepeating)
		}
		return err
	}

	if restartLoop {
		// If we were fetching tables before we called RunScript,
		// we should try to make sure the loop is restored.
		err = client.fetchTablesWhileLocked(context, FetchRepeating)
		if err != nil {
			return err
		}
	}

	return nil
}

// This is thread-safe
func (client *Client) handleScriptChanges(resp *consolepb2.ExecuteCommandResponse) {
	client.handleFieldChanges(resp.Changes)
}

// This is thread-safe
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
