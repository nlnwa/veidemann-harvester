// Copyright © 2017 National Library of Norway.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"broprox"
	"fmt"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"log"
)

func NewControllerClient() (broprox.ControllerClient, *grpc.ClientConn) {
	address := viper.GetString("controllerAddress")
	fmt.Printf("Connecting to %s\n", address)

	dialOptions := []grpc.DialOption{grpc.WithInsecure()}
	dialOptions = AddCredentials(dialOptions)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, dialOptions...)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	//defer conn.Close()
	c := broprox.NewControllerClient(conn)
	return c, conn
}

func NewStatusClient() (broprox.StatusClient, *grpc.ClientConn) {
	address := viper.GetString("controllerAddress")
	fmt.Printf("Connecting to %s\n", address)
	// Set up a connection to the server.
	dialOptions := []grpc.DialOption{grpc.WithInsecure()}
	dialOptions = AddCredentials(dialOptions)

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, dialOptions...)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	//defer conn.Close()
	c := broprox.NewStatusClient(conn)
	return c, conn
}

type bearerTokenCred struct {
	tokenType string
	token     string
}

func AddCredentials(opts []grpc.DialOption) []grpc.DialOption {
	a := NewAuth()
	a.CheckStoredAccessToken()
	if a.rawIdToken == "" {
		return opts
	}

	var bt credentials.PerRPCCredentials = &bearerTokenCred{a.oauth2Token.TokenType, a.rawIdToken}
	return append(opts, grpc.WithPerRPCCredentials(bt))
}

func (b bearerTokenCred) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": b.tokenType + " " + b.token,
	}, nil
}

func (b bearerTokenCred) RequireTransportSecurity() bool {
	return false
}
