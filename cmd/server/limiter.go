package main

import (
	"net/http"

	"github.com/didip/tollbooth"
	"github.com/didip/tollbooth/config"
	"github.com/julienschmidt/httprouter"
	"github.com/urfave/negroni"
)

// From: https://github.com/didip/tollbooth/tree/master/thirdparty/tollbooth_negroni

func LimitHandler(limiter *config.Limiter) negroni.HandlerFunc {
	return negroni.HandlerFunc(func(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
		httpError := tollbooth.LimitByRequest(limiter, r)
		if httpError != nil {
			writeError(w, httpError.StatusCode, ErrorResp{Message: httpError.Message})
			return
		} else {
			next(w, r)
		}
	})
}

func AttachLimitHandler(handler httprouter.Handle, limiter *config.Limiter) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		httpError := tollbooth.LimitByRequest(limiter, r)
		if httpError != nil {
			writeError(w, httpError.StatusCode, ErrorResp{Message: httpError.Message})
			return
		}
		handler(w, r, ps)
	}
}
