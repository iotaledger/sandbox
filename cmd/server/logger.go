package main

import (
	"net/http"
	"time"

	"github.com/urfave/negroni"
	"go.uber.org/zap"
)

type LoggerMiddleware struct {
	logger *zap.Logger
}

func NewLoggerMiddleware(l *zap.Logger) (*LoggerMiddleware, error) {
	mw := &LoggerMiddleware{}

	if l == nil {
		logger, err := zap.NewDevelopment()
		if err != nil {
			return nil, err
		}
		mw.logger = logger
	} else {
		mw.logger = l
	}

	return mw, nil
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
