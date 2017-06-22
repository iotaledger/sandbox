package main

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/iotaledger/sandbox/auth"
)

var headerSplitter = regexp.MustCompile(`\s+`)

// AuthMiddleware adds the `authorization` value to the request context,
// so that handlers after it can check for the authentication status of
// the current request.
type AuthMiddleware struct {
	store auth.AuthStore
}

func NewAuthMiddleware(as auth.AuthStore) *AuthMiddleware {
	return &AuthMiddleware{store: as}
}

// parseAuthorizationHeader expects a string in the form of
// `token authtoken` and returns the `authtoken` or an empty string.
func parseAuthorizationHeader(h string) string {
	parts := headerSplitter.Split(h, 2)
	if len(parts) != 2 {
		return ""
	} else if strings.ToLower(parts[0]) != "token" {
		return ""
	}

	return strings.TrimSpace(parts[1])
}

func (a *AuthMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	hdr := r.Header.Get(http.CanonicalHeaderKey("Authorization"))
	token := parseAuthorizationHeader(hdr)
	if token == "" {
		next(w, r)
		return
	}

	auth, err := a.store.ValidToken(r.Context(), token)
	if err != nil {
		fmt.Printf("ValidToken error: %s\n", err)
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: "could not authenticate, please try again later"})
		return
	}

	ogCtx := r.Context()
	freshCtx := context.WithValue(ogCtx, "authorization", auth)
	next(w, r.WithContext(freshCtx))
}
