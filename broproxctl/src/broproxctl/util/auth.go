// Copyright © 2017 National Library of Norway
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
	"context"
	"fmt"
	"github.com/coreos/go-oidc"
	"github.com/ghodss/yaml"
	"github.com/spf13/viper"
	"golang.org/x/oauth2"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"time"
)

type Auth struct {
	clientID     string
	clientSecret string
	redirectURI  string

	idTokenVerifier *oidc.IDTokenVerifier
	provider        *oidc.Provider

	// Does the provider use "offline_access" scope to request a refresh token
	// or does it use "access_type=offline" (e.g. Google)?
	offlineAsScope bool

	client *http.Client
	state  string
}

func (a *Auth) Init(idpUrl string) {
	a.offlineAsScope = true
	a.clientID = "veidemann-cli"
	a.clientSecret = "cli-app-secret"
	a.redirectURI = "urn:ietf:wg:oauth:2.0:oob"

	a.client = http.DefaultClient
	ctx := oidc.ClientContext(context.Background(), a.client)

	// Initialize a provider by specifying dex's issuer URL.
	p, err := oidc.NewProvider(ctx, idpUrl)
	if err != nil {
		log.Fatal(err)
	}
	a.provider = p
	oc := oidc.Config{ClientID: a.clientID}
	a.idTokenVerifier = a.provider.Verifier(&oc)
}

func (a *Auth) oauth2Config() *oauth2.Config {
	return &oauth2.Config{
		ClientID:     a.clientID,
		ClientSecret: a.clientSecret,
		Endpoint:     a.provider.Endpoint(),
		Scopes:       []string{oidc.ScopeOpenID, "profile", "email", "groups", "offline_access", "audience:server:client_id:veidemann-admin"},
		RedirectURL:  a.redirectURI,
	}
}

func (a *Auth) CreateAuthCodeURL() string {
	a.state = RandStringBytesMaskImprSrc(16)
	viper.Set("nonce", a.state)

	nonce := oidc.Nonce(a.state)
	return a.oauth2Config().AuthCodeURL(a.state, nonce)
}

func (a *Auth) Openbrowser(authCodeURL string) {
	var err error

	switch runtime.GOOS {
	case "linux":
		err = exec.Command("xdg-open", authCodeURL).Start()
	case "windows":
		err = exec.Command("rundll32", "url.dll,FileProtocolHandler", authCodeURL).Start()
	case "darwin":
		err = exec.Command("open", authCodeURL).Start()
	default:
		err = fmt.Errorf("unsupported platform")
	}
	if err != nil {
		log.Fatal(err)
	}
}

func (a *Auth) VerifyCode(code string) (string, *oidc.IDToken) {
	ctx := context.Background()
	oauth2Token, err := a.oauth2Config().Exchange(ctx, code)
	if err != nil {
		log.Fatal(err)
	}

	// Extract the ID Token from OAuth2 token.
	rawIDToken, ok := oauth2Token.Extra("id_token").(string)
	if !ok {
		log.Fatal("No token found")
	}

	// Parse and verify ID Token payload.
	idToken, err := a.idTokenVerifier.Verify(ctx, rawIDToken)
	if err != nil {
		log.Fatal(err)
	}
	viper.Set("idToken", rawIDToken)

	if idToken.Nonce != viper.GetString("nonce") {
		log.Fatal("Nonce did not match")
	}

	return rawIDToken, idToken
}

// Extract custom claims.
type Claims struct {
	Email    string   `json:"email"`
	Verified bool     `json:"email_verified"`
	Groups   []string `json:"groups"`
	Name     string   `json:"name"`
}

func (a *Auth) ExtractClaims(idToken *oidc.IDToken) (claims Claims) {
	if err := idToken.Claims(&claims); err != nil {
		log.Fatal(err)
	}
	return claims
}

func GetRawIdToken(ipdUrl string) string {
	rawIdToken := viper.GetString("idToken")
	if rawIdToken == "" {
		return ""
	}

	var a Auth
	a.Init(ipdUrl)
	// Parse and verify ID Token payload.
	_, err := a.idTokenVerifier.Verify(context.Background(), rawIdToken)
	if err != nil {
		log.Fatal(err)
	}
	return rawIdToken
}

type config struct {
	ControllerAddress string `json:"controllerAddress"`
	Idp               string `json:"idp"`
	IdToken           string `json:"idToken"`
	Nonce             string `json:"nonce"`
}

func WriteConfig() {
	log.Println("Writing config")

	c := config{
		viper.GetString("controllerAddress"),
		viper.GetString("idp"),
		viper.GetString("idToken"),
		viper.GetString("nonce"),
	}

	y, err := yaml.Marshal(c)
	if err != nil {
		log.Fatalf("err: %v\n", err)
		os.Exit(2)
	}
	f, err := os.Create(viper.ConfigFileUsed())
	if err != nil {
		log.Fatalf("Could not create file '%s': %v", viper.ConfigFileUsed(), err)
		os.Exit(2)
	}
	f.Chmod(0600)
	defer f.Close()
	f.Write(y)
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var src = rand.NewSource(time.Now().UnixNano())

func RandStringBytesMaskImprSrc(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}
