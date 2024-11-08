//go:build as_proxy

// Copyright 2014-2022 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aerospike

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"runtime/debug"
	"strings"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/aerospike/aerospike-client-go/v7/logger"
	auth "github.com/aerospike/aerospike-client-go/v7/proto/auth"
	"github.com/aerospike/aerospike-client-go/v7/types"
)

type authInterceptor struct {
	clnt   *ProxyClient
	closer chan struct{}

	expiry    time.Time
	fullToken string // "Bearer <token>"
}

func newAuthInterceptor(clnt *ProxyClient) (*authInterceptor, Error) {
	interceptor := &authInterceptor{
		clnt:   clnt,
		closer: make(chan struct{}),
	}

	err := interceptor.scheduleRefreshToken()
	if err != nil {
		return nil, err
	}

	return interceptor, nil
}

func (interceptor *authInterceptor) close() {
	if interceptor.active() {
		close(interceptor.closer)
	}
}

func (interceptor *authInterceptor) active() bool {
	active := true
	select {
	case _, active = <-interceptor.closer:
	default:
	}
	return active
}

func (interceptor *authInterceptor) scheduleRefreshToken() Error {
	err := interceptor.refreshToken()
	if err != nil {
		return err
	}

	// launch the refresher go routine
	go interceptor.tokenRefresher()

	return nil
}

func (interceptor *authInterceptor) tokenRefresher() {
	// make sure the goroutine is restarted if something panics downstream
	defer func() {
		if r := recover(); r != nil {
			logger.Logger.Error("Interceptor refresh goroutine crashed: %s", debug.Stack())
			go interceptor.tokenRefresher()
		}
	}()

	// provide 5 secs of buffer before expiry due to network latency
	wait := interceptor.expiry.Sub(time.Now()) - 5*time.Second
	ticker := time.NewTicker(wait)
	defer ticker.Stop()

	for {
		ticker.Reset(wait)
		select {
		case <-ticker.C:
			err := interceptor.refreshToken()
			if err != nil {
				wait = time.Second
			} else {
				wait = interceptor.expiry.Sub(time.Now()) - 5*time.Second
			}

		case <-interceptor.closer:
			// channel closed; return from the goroutine
			return
		}
	}
}

func (interceptor *authInterceptor) refreshToken() Error {
	err := interceptor.login()
	if err != nil {
		return err
	}

	interceptor.clnt.setAuthToken(interceptor.fullToken)

	return nil
}

func (interceptor *authInterceptor) RequireTransportSecurity() bool {
	return true
}

func (interceptor *authInterceptor) Unary() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		return invoker(interceptor.attachToken(ctx), method, req, reply, cc, opts...)
	}
}

func (interceptor *authInterceptor) Stream() grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		return streamer(interceptor.attachToken(ctx), desc, cc, method, opts...)
	}
}

func (interceptor *authInterceptor) attachToken(ctx context.Context) context.Context {
	token := interceptor.clnt.token()
	return metadata.AppendToOutgoingContext(ctx, "Authorization", token)
}

func (interceptor *authInterceptor) login() Error {
	conn, err := interceptor.clnt.createGrpcConn(true)
	if err != nil {
		return err
	}
	defer conn.Close()

	req := auth.AerospikeAuthRequest{
		Username: interceptor.clnt.clientPolicy.User,
		Password: interceptor.clnt.clientPolicy.Password,
	}

	client := auth.NewAuthServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), interceptor.clnt.clientPolicy.Timeout)
	defer cancel()

	res, gerr := client.Get(ctx, &req)
	if gerr != nil {
		return newGrpcError(false, gerr, gerr.Error())
	}

	claims := strings.Split(res.GetToken(), ".")
	decClaims, gerr := base64.RawURLEncoding.DecodeString(claims[1])
	if gerr != nil {
		return newGrpcError(false, gerr, "Invalid token encoding. Expected base64.")
	}

	tokenMap := make(map[string]interface{}, 8)
	gerr = json.Unmarshal(decClaims, &tokenMap)
	if gerr != nil {
		return newError(types.PARSE_ERROR, "Invalid token encoding. Expected json.")
	}

	expiryToken, ok := tokenMap["exp"].(float64)
	if !ok {
		return newError(types.PARSE_ERROR, "Invalid expiry value. Expected float64.")
	}

	iat, ok := tokenMap["iat"].(float64)
	if !ok {
		return newError(types.PARSE_ERROR, "Invalid iat value. Expected float64.")

	}

	ttl := time.Duration(expiryToken-iat) * time.Second
	if ttl <= 0 {
		return newError(types.PARSE_ERROR, "Invalid token values. token 'iat' > 'exp'")
	}

	// Set expiry based on local clock.
	expiry := time.Now().Add(ttl)
	interceptor.fullToken = "Bearer " + res.GetToken()
	interceptor.expiry = expiry

	return nil
}
