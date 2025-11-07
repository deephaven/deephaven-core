package client

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v8/arrow/flight"
	configpb2 "github.com/deephaven/deephaven-core/go/internal/proto/config"
	sessionpb2 "github.com/deephaven/deephaven-core/go/internal/proto/session"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"log"
	"strconv"
	"sync"
	"time"
)

// makeAuthString creates an authentication string from an authentication type and an authentication token.
func makeAuthString(authType string, authToken string) string {
	if authType == "Anonymous" {
		return authType
	} else if authType == "Basic" {
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(authToken))
	} else {
		return authType + " " + authToken
	}
}

// withAuth returns a context decorated with authentication data.
func withAuth(ctx context.Context, authString string) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", authString))
}

// withAuthToken returns a context decorated with an authentication token.
func withAuthToken(ctx context.Context, token []byte) context.Context {
	return withAuth(ctx, "Bearer "+string(token))
}

// requestToken requests a new token from flight.
func requestToken(handshakeClient flight.FlightService_HandshakeClient, authType string, authToken []byte) ([]byte, error) {

	war := sessionpb2.WrappedAuthenticationRequest{
		Type:    authType,
		Payload: authToken,
	}
	payload, err := proto.Marshal(&war)
	if err != nil {
		return nil, err
	}
	handshakeReq := flight.HandshakeRequest{Payload: []byte(payload)}

	err = handshakeClient.Send(&handshakeReq)

	if err != nil {
		return nil, err
	}

	handshakeResp, err := handshakeClient.Recv()

	if err != nil {
		return nil, err
	}

	return handshakeResp.Payload, nil
}

// tokenResp protects the current session token (or an error in getting the session token).
type tokenResp struct {
	Lock  sync.Mutex
	Token []byte
	Error error
}

// getToken returns the current token, or an error if an error has occurred at some point.
func (tk *tokenResp) getToken() ([]byte, error) {
	tk.Lock.Lock()
	defer tk.Lock.Unlock()
	if tk.Error != nil {
		return nil, tk.Error
	} else {
		return tk.Token, nil
	}
}

// setToken sets the session token to a new value.
func (tk *tokenResp) setToken(tok []byte) {
	tk.Lock.Lock()
	tk.Token = tok
	tk.Error = nil
	tk.Lock.Unlock()
}

// setError sets an error value for the session token.
func (tk *tokenResp) setError(err error) {
	tk.Lock.Lock()
	tk.Error = err
	tk.Lock.Unlock()
}

// A tokenManager stores the current client token and sends periodic keepalive messages to refresh the client token.
type tokenManager struct {
	token *tokenResp // the actual client token, which gets periodically updated.
	close func() error
}

// getToken returns the current token, or an error if an error has occurred at some point.
func (tr *tokenManager) getToken() ([]byte, error) {
	return tr.token.getToken()
}

// withToken attaches the current session token to a context as metadata.
func (tr *tokenManager) withToken(ctx context.Context) (context.Context, error) {
	tok, err := tr.getToken()

	if err != nil {
		return nil, err
	}

	return withAuthToken(ctx, tok), nil
}

func (tr *tokenManager) Close() error {
	return tr.close()
}

// newTokenManager creates a tokenManager that begins a background goroutine that continually refreshes
// the token so that it does not time out.
//
// authType is the type of authentication to use.  This can be 'Anonymous', 'Basic', or any custom-built
// authenticator in the server, such as "io.deephaven.authentication.psk.PskAuthenticationHandler",  The default is 'Anonymous'.
// To see what authentication methods are available on the Deephaven server, navigate to: http://<host>:<port>/jsapi/authentication/.
//
// authToken is the authentication token string. When authType is 'Basic', it must be
// "user:password"; when auth_type is DefaultAuth, it will be ignored; when auth_type is a custom-built
// authenticator, it must conform to the specific requirement of the authenticator.
func newTokenManager(ctx context.Context, fs *flightStub, cfg configpb2.ConfigServiceClient, authType string, authToken string) (*tokenManager, error) {
	handshakeClient, err := fs.handshake(ctx)

	if err != nil {
		return nil, err
	}

	tkn, err := requestToken(handshakeClient, authType, []byte(authToken))

	if err != nil {
		return nil, err
	}

	ac, err := cfg.GetConfigurationConstants(withAuthToken(ctx, tkn), &configpb2.ConfigurationConstantsRequest{})

	if err != nil {
		return nil, err
	}

	sessionDurationStr, ok := ac.ConfigValues[TokenTimeoutConfigConstant]

	if !ok {
		return nil, errors.New(fmt.Sprintf("server configuration constants do not contain: %v", TokenTimeoutConfigConstant))
	}

	maxTimeoutMillis, err := strconv.Atoi(sessionDurationStr.GetStringValue())

	if err != nil {
		return nil, err
	}

	timeout := time.Duration(maxTimeoutMillis/2) * time.Millisecond

	token := &tokenResp{Token: tkn}
	done := make(chan bool)
	ticker := time.NewTicker(timeout)

	go func() {
		for {
			select {
			case <-done:
				// Make sure that nobody accidentally tries
				// to use a token after the client has closed.
				token.setError(ErrClosedClient)
				return
			case <-ticker.C:
				oldToken, err := token.getToken()

				var tkn []byte

				if err == nil {
					tkn, err = requestToken(handshakeClient, "Bearer", oldToken)
				} else {
					log.Println("Old token has an error during token update.  Attempting to acquire a fresh token.  err=", err)
					tkn, err = requestToken(handshakeClient, authType, []byte(authToken))
				}

				if err != nil {
					token.setError(err)
					log.Println("Error when updating token.  err=", err)
				} else {
					token.setToken(tkn)
				}
			}
		}
	}()

	ref := &tokenManager{
		token: token,
		close: func() error {
			ticker.Stop()
			done <- true
			return handshakeClient.CloseSend()
		},
	}

	return ref, nil
}
