package client

import (
	"context"
	"sync"
	"time"

	sessionpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/session"
	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"

	"google.golang.org/grpc/metadata"
)

type tokenResp struct {
	Lock  sync.Mutex
	Token []byte
	Error error
}

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

func (tk *tokenResp) setToken(tok []byte) {
	tk.Lock.Lock()
	tk.Token = tok
	tk.Lock.Unlock()
}

func (tk *tokenResp) setError(err error) {
	tk.Lock.Lock()
	tk.Error = err
	tk.Lock.Unlock()
}

// Stores the current client token and sends periodic keepalive messages
type refresher struct {
	ctx         context.Context
	sessionStub sessionpb2.SessionServiceClient

	token *tokenResp

	cancelCh <-chan struct{}

	timeoutMillis int64
}

func (ref *refresher) refreshLoop() {
	for {
		select {
		case <-ref.cancelCh:
			return
		case <-time.After(time.Duration(ref.timeoutMillis) * time.Millisecond):
			err := ref.refresh()
			if err != nil {
				return
			}
		}
	}
}

func startRefresher(ctx context.Context, sessionStub sessionpb2.SessionServiceClient, token *tokenResp, cancelCh chan struct{}) error {
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

	token *tokenResp

	cancelCh chan<- struct{}
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

func (hs *sessionStub) getToken() ([]byte, error) {
	return hs.token.getToken()
}

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

func (hs *sessionStub) Close() {
	if hs.cancelCh != nil {
		close(hs.cancelCh)
		hs.cancelCh = nil
	}
}
