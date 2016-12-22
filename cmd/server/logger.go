package main

import (
	"net/http"
	"os"
	"time"

	"github.com/uber-go/zap"
	"github.com/urfave/negroni"
)

type LoggerMiddleware struct {
	logger zap.Logger
}

func NewLoggerMiddleware() *LoggerMiddleware {
	mw := &LoggerMiddleware{}
	if os.Getenv("DEBUG") == "1" {
		mw.logger = zap.New(zap.NewJSONEncoder(), zap.DebugLevel)
	} else {
		mw.logger = zap.New(zap.NewJSONEncoder())
	}

	return mw
}

func (mw *LoggerMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	s := time.Now()
	next(w, r)
	res := w.(negroni.ResponseWriter)

	mw.logger.Info("completed request",
		zap.Duration("duration", time.Since(s)),
		zap.Int("status", res.Status()),
		zap.String("remote", r.RemoteAddr),
		zap.String("path", r.RequestURI),
	)
}
