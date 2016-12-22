package main

import (
	"net/http"

	"github.com/urfave/negroni"
)

func hasCT(ct string, cts []string) bool {
	for _, c := range cts {
		if c == ct {
			return true
		}
	}

	return false
}

func ContentTypeEnforcer(cts ...string) negroni.HandlerFunc {
	return negroni.HandlerFunc(func(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
		ct := r.Header.Get(http.CanonicalHeaderKey("Content-Type"))
		if hasCT(ct, cts) || (r.Method != "POST" && r.Method != "PUT") {
			next(w, r)
		} else {
			http.Error(w, http.StatusText(http.StatusUnsupportedMediaType), http.StatusUnsupportedMediaType)
		}
	})
}
