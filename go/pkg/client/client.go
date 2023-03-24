// This package allows you to interface with a Deephaven server over a network connection using Go.
// It can upload, manipulate, and download tables, among other features.
//
// To get started, use client.NewClient to connect to the server. The Client can then be used to perform operations.
// See the provided examples in the examples/ folder or the individual code documentation for more.
//
// Online docs for the client can be found at https://pkg.go.dev/github.com/deephaven/deephaven-core/go/pkg/client
//
// The Go API uses Records from the Apache Arrow package as tables.
// The docs for the Arrow package can be found at the following link:
// https://pkg.go.dev/github.com/apache/arrow/go/v8
//
// All methods for all structs in this package are goroutine-safe unless otherwise specified.
package client

import (
	"context"
	"errors"
	"log"
	"sync"

	apppb2 "github.com/deephaven/deephaven-core/go/internal/proto/application"
	configpb2 "github.com/deephaven/deephaven-core/go/internal/proto/config"
	consolepb2 "github.com/deephaven/deephaven-core/go/internal/proto/console"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ErrClosedClient is returned as an error when trying to perform a network operation on a client that has been closed.
var ErrClosedClient = errors.New("client is closed")

// Maintains a connection to a Deephaven server.
// It can be used to run scripts, create new tables, execute queries, etc.
// Check the various methods of Client to learn more.
type Client struct {
	// This lock guards isOpen.
	// Other functionality does not need a lock, since the gRPC interface is already thread-safe,
	// the tables array has its own lock, and the session token also has its own lock.
	lock   sync.Mutex
	isOpen bool // False if Close has been called (i.e. the client can no longer perform operations).

	grpcChannel *grpc.ClientConn

	suppressTableLeakWarning bool // When true, this disables the TableHandle finalizer warning.

	*sessionStub
	*consoleStub
	*flightStub
	*tableStub
	*inputTableStub

	appServiceClient apppb2.ApplicationServiceClient
	ticketFact       *ticketFactory
	tokenMgr         *tokenManager
}

// NewClient starts a connection to a Deephaven server.
//
// The client should be closed using Close() after it is done being used.
//
// Keepalive messages are sent automatically by the client to the server at a regular interval
// so that the connection remains open. The provided context is saved and used to send keepalive messages.
//
// host, port, and auth are used to connect to the Deephaven server.  host and port are the Deephaven server host and port.
// auth is the authorization string used to get the first token.  Examples:
//   - "Anonymous" is used for anonymous authentication.
//   - "io.deephaven.authentication.psk.PskAuthenticationHandler <password>" is used for PSK authentication
//
// If auth is set to an empty string, anonymous authentication is used.
// To see what authentication methods are available on the Deephaven server, navigate to: http://<host>:<port>/jsapi/authentication/.
//
// The option arguments can be used to specify other settings for the client.
// See the With<XYZ> methods (e.g. WithConsole) for details on what options are available.
func NewClient(ctx context.Context, host string, port string, auth string, options ...ClientOption) (client *Client, err error) {
	defer func() {
		if err != nil && client != nil {
			e := client.Close()
			log.Println("Error when closing failed client: ", e)
		}
	}()

	grpcChannel, err := grpc.Dial(host+":"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	opts := newClientOptions(options...)

	client = &Client{grpcChannel: grpcChannel, isOpen: true}

	client.suppressTableLeakWarning = opts.suppressTableLeakWarning

	client.ticketFact = newTicketFactory()

	client.flightStub, err = newFlightStub(client, host, port)
	if err != nil {
		return nil, err
	}

	cfgClient := configpb2.NewConfigServiceClient(grpcChannel)

	if auth == "" {
		auth = "Anonymous"
	}

	client.tokenMgr, err = newTokenManager(ctx, client.flightStub, cfgClient, auth)
	if err != nil {
		return nil, err
	}

	client.sessionStub, err = newSessionStub(client)
	if err != nil {
		return nil, err
	}

	client.consoleStub, err = newConsoleStub(ctx, client, opts.scriptLanguage)
	if err != nil {
		return nil, err
	}

	client.tableStub = newTableStub(client)

	client.inputTableStub = newInputTableStub(client)

	client.appServiceClient = apppb2.NewApplicationServiceClient(client.grpcChannel)

	return client, nil
}

// Closed checks if the client is closed, i.e. it can no longer perform operations on the server.
func (client *Client) Closed() bool {
	client.lock.Lock()
	defer client.lock.Unlock()

	return !client.isOpen
}

// ExecSerial executes a query graph on the server and returns the resulting tables.
//
// This function makes a request for each table operation in the query graph.
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
//
// See the TableOps example and the QueryNode docs for more details on how this method should be used.
func (client *Client) ExecSerial(ctx context.Context, nodes ...QueryNode) ([]*TableHandle, error) {
	return execSerial(ctx, client, nodes)
}

// ExecBatch executes a query graph on the server and returns the resulting tables.
//
// All of the operations in the query graph will be performed in a single request,
// so ExecBatch is usually more efficient than ExecSerial.
//
// If this function completes successfully,
// the number of tables returned will always match the number of query nodes passed.
// The first table in the returned list corresponds to the first node argument,
// the second table in the returned list corresponds to the second node argument,
// etc.
//
// This may return a QueryError if the query is invalid.
//
// See the TableOps example and the QueryNode docs for more details on how this method should be used.
func (client *Client) ExecBatch(ctx context.Context, nodes ...QueryNode) ([]*TableHandle, error) {
	return execBatch(client, ctx, nodes)
}

// lockIfOpen returns true if the client is open, i.e. it can be used to perform operations.
// If this function returns true, it will acquire a lock for the client,
// which will prevent it from being closed.
func (client *Client) lockIfOpen() bool {
	client.lock.Lock()
	if client.isOpen {
		return true
	}
	client.lock.Unlock()
	return false
}

// Close closes the connection to the server and frees any associated resources.
// Once this method is called, the client and any TableHandles from it cannot be used.
func (client *Client) Close() error {
	if !client.lockIfOpen() {
		return nil
	}
	defer client.lock.Unlock()

	client.isOpen = false

	if client.tokenMgr != nil {
		err := client.tokenMgr.Close()
		if err != nil {
			log.Println("unable to close client:", err.Error())
			return err
		}
	}

	// This is logged because most of the time this method is used with defer,
	// which will discard the error value.
	if client.flightStub != nil {
		err := client.flightStub.Close()
		if err != nil {
			log.Println("unable to close client:", err.Error())
			return err
		}
	}

	if client.grpcChannel != nil {
		client.grpcChannel.Close()
		client.grpcChannel = nil
	}

	return nil
}

// RunScript executes a script on the deephaven server.
//
// The script language depends on the argument passed to WithConsole when creating the client.
// If WithConsole was not provided when creating the client, this method will return ErrNoConsole.
func (client *Client) RunScript(ctx context.Context, script string) error {
	if client.consoleStub.consoleId == nil {
		return ErrNoConsole
	}

	ctx, err := client.consoleStub.client.tokenMgr.withToken(ctx)
	if err != nil {
		return err
	}

	req := consolepb2.ExecuteCommandRequest{ConsoleId: client.consoleStub.consoleId, Code: script}
	_, err = client.consoleStub.stub.ExecuteCommand(ctx, &req)
	if err != nil {
		return err
	}

	return nil
}

// clientOptions holds a set of configurable options to use when creating a client with NewClient.
type clientOptions struct {
	scriptLanguage           string // The language to use for server-side scripts. Empty string means no scripts can be run.
	suppressTableLeakWarning bool   // When true, disables the TableHandle finalizer warning.
}

// newClientOptions applies all of the provided options and returns the resulting struct of settings.
func newClientOptions(opts ...ClientOption) clientOptions {
	options := clientOptions{}
	for _, opt := range opts {
		opt.apply(&options)
	}
	return options
}

// A ClientOption configures some aspect of a client connection when passed to NewClient.
// See the With<XYZ> methods for possible client options.
type ClientOption interface {
	// apply sets the relevant option in the clientOptions struct.
	apply(opts *clientOptions)
}

// A funcDialOption wraps a function that will apply a client option.
// Inspiration from the grpc-go package.
type funcDialOption struct {
	f func(opts *clientOptions)
}

func (opt funcDialOption) apply(opts *clientOptions) {
	opt.f(opts)
}

// WithConsole allows the client to run scripts on the server using the RunScript method and bind tables to variables using BindToVariable.
//
// The script language can be either "python" or "groovy", and must match the language used on the server.
func WithConsole(scriptLanguage string) ClientOption {
	return funcDialOption{func(opts *clientOptions) {
		opts.scriptLanguage = scriptLanguage
	}}
}

// WithNoTableLeakWarning disables the automatic TableHandle leak check.
//
// Normally, a warning is printed whenever a TableHandle is forgotten without calling Release on it,
// and a GC finalizer automatically frees the table.
// However, TableHandles are automatically released by the server whenever a client connection closes.
// So, it can be okay for short-lived clients that don't create large tables to forget their TableHandles
// and rely on them being freed when the client closes.
//
// There is no guarantee on when the GC will run, so long-lived clients that forget their TableHandles
// can end up exhausting the server's resources before any of the handles are GCed automatically.
func WithNoTableLeakWarning() ClientOption {
	return funcDialOption{func(opts *clientOptions) {
		opts.suppressTableLeakWarning = true
	}}
}
