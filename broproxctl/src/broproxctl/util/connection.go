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
	"google.golang.org/grpc"
	"log"
	"google.golang.org/grpc/credentials"
	"golang.org/x/net/context"
)

func NewControllerClient(idToken string) (broprox.ControllerClient, *grpc.ClientConn) {
	address := viper.GetString("controllerAddress")
	fmt.Printf("Connecting to %s\n", address)
	// Set up a connection to the server.
	var perRPCCreds grpc.DialOption
	if idToken == "" {
		perRPCCreds = grpc.WithInsecure()
	} else {
		perRPCCreds = grpc.WithPerRPCCredentials(NewFromIdToken(idToken))
	}
	conn, err := grpc.Dial(address, grpc.WithInsecure(), perRPCCreds)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	//defer conn.Close()
	c := broprox.NewControllerClient(conn)
	return c, conn
}

func NewStatusClient(idToken string) (broprox.StatusClient, *grpc.ClientConn) {
	address := viper.GetString("controllerAddress")
	fmt.Printf("Connecting to %s\n", address)
	// Set up a connection to the server.
	var perRPCCreds grpc.DialOption
	if idToken == "" {
		perRPCCreds = grpc.WithInsecure()
	} else {
		perRPCCreds = grpc.WithPerRPCCredentials(NewFromIdToken(idToken))
	}
	conn, err := grpc.Dial(address, grpc.WithInsecure(), perRPCCreds)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	//defer conn.Close()
	c := broprox.NewStatusClient(conn)
	return c, conn
}


type bearerTokenCred struct {
	token string
}

func NewFromIdToken(token string) credentials.PerRPCCredentials {
	return bearerTokenCred{token}
}

func (b bearerTokenCred) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"bearer-token": b.token,
	}, nil
}

func (b bearerTokenCred) RequireTransportSecurity() bool {
	return false
}
