package client

import (
	"context"
	"sync"
	"time"

	sessionpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/session"
	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"

	"google.golang.org/grpc/metadata"
)

// tokenResp thread-safely protects the current session token (or an error in getting the session token).
type tokenResp struct {
	Lock  sync.Mutex
	Token []byte
	Error error
}

// getToken thread-safely returns the current token, or an error if an error has occurred at some point.
func (tk *tokenResp) getToken() ([]byte, error) {
	tk.Lock.Lock()
	defer tk.Lock.Unlock()
	if tk.Error != nil {
		return nil, tk.Error
	} else {
		token := make([]byte, len(tk.Token))
		copy(token, tk.Token)
		return token, nil
	}
}

// setToken thread-safely sets the session token to a new value.
func (tk *tokenResp) setToken(tok []byte) {
	tk.Lock.Lock()
	tk.Token = tok
	tk.Lock.Unlock()
}

// setError thread-safely sets an error value for the session token.
func (tk *tokenResp) setError(err error) {
	tk.Lock.Lock()
	tk.Error = err
	tk.Lock.Unlock()
}

// A refresher stores the current client token and sends periodic keepalive messages to refresh the client token.
type refresher struct {
	ctx         context.Context
	sessionStub sessionpb2.SessionServiceClient

	token *tokenResp // the actual client token, which gets periodically updated.

	cancelCh <-chan struct{} // if this channel closes, the refresher should stop.

	timeoutMillis int64 // time (in milliseconds) before the token should be refreshed again.
}

// refreshLoop is an endless loop that will update the token when necessary.
// It can be canceled by closing the cancelCh channel.
func (ref *refresher) refreshLoop() {
	for {
		select {
		case <-ref.cancelCh:
			return
		case <-time.After(time.Duration(ref.timeoutMillis) * time.Millisecond):
			err := ref.refresh()
			if err != nil {
				// refresh() stores the error in the tokenResp struct, so it can be handled
				// appropriately by all the client methods that need a token.
				// Thus, we can discard it here.
				return
			}
		}
	}
}

// startRefresher begins a background goroutine that continually refreshes the passed token so that it does not time out.
func startRefresher(ctx context.Context, sessionStub sessionpb2.SessionServiceClient, token *tokenResp, cancelCh <-chan struct{}) error {
	handshakeReq := &sessionpb2.HandshakeRequest{AuthProtocol: 1, Payload: [](byte)("hello godeephaven")}
	handshakeResp, err := sessionStub.NewSession(ctx, handshakeReq)
	if err != nil {
		return err
	}

	ctx = metadata.NewOutgoingContext(context.Background(), metadata.Pairs("deephaven_session_id", string(handshakeResp.SessionToken)))

	token.setToken(handshakeResp.SessionToken)

	timeoutMillis := handshakeResp.TokenExpirationDelayMillis / 2

	ref := refresher{
		ctx:         ctx,
		sessionStub: sessionStub,

		token: token,

		cancelCh: cancelCh,

		timeoutMillis: timeoutMillis,
	}

	go ref.refreshLoop()

	return nil
}

// refresh refreshes the client token. It makes a RefreshSessionToken request,
// and then updates the token struct.
func (ref *refresher) refresh() error {
	oldToken, err := ref.token.getToken()
	if err != nil {
		return err
	}

	handshakeReq := &sessionpb2.HandshakeRequest{AuthProtocol: 0, Payload: oldToken}
	handshakeResp, err := ref.sessionStub.RefreshSessionToken(ref.ctx, handshakeReq)

	if err != nil {
		ref.token.setError(err)
		return err
	} else {
		ref.token.setToken(handshakeResp.SessionToken)
	}

	ref.timeoutMillis = handshakeResp.TokenExpirationDelayMillis / 2

	return nil
}

// sessionStub wraps gRPC calls from session.proto, which includes the session/keepalive handling.
type sessionStub struct {
	client *Client
	stub   sessionpb2.SessionServiceClient

	token *tokenResp // the client token, which is also shared with a refresher.

	cancelCh chan<- struct{} // closing this channel will stop the refresher.
}

// Performs the first handshake to get a client token.
func newSessionStub(ctx context.Context, client *Client) (sessionStub, error) {
	stub := sessionpb2.NewSessionServiceClient(client.grpcChannel)

	cancelCh := make(chan struct{})

	tokenResp := &tokenResp{}

	err := startRefresher(ctx, stub, tokenResp, cancelCh)
	if err != nil {
		return sessionStub{}, err
	}

	hs := sessionStub{
		client: client,
		stub:   stub,

		token: tokenResp,

		cancelCh: cancelCh,
	}

	return hs, nil
}

// getToken returns the current session token in a thread-safe way.
// It may return an error if there has been an error at some point in the past while refreshing the token.
func (hs *sessionStub) getToken() ([]byte, error) {
	return hs.token.getToken()
}

// release releases a table ticket to free its resources on the server.
func (hs *sessionStub) release(ctx context.Context, ticket *ticketpb2.Ticket) error {
	ctx, err := hs.client.withToken(ctx)
	if err != nil {
		return err
	}

	req := sessionpb2.ReleaseRequest{Id: ticket}
	_, err = hs.stub.Release(ctx, &req)
	if err != nil {
		return err
	}
	return nil
}

// Close closes the session stub and frees any associated resources.
// The session stub should not be used after calling this function.
// The client lock should be held when calling this function.
func (hs *sessionStub) Close() {
	if hs.cancelCh != nil {
		close(hs.cancelCh)
		hs.cancelCh = nil
	}
}
