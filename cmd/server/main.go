package main

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/iotaledger/sandbox/job"

	//"github.com/eapache/go-resiliency/breaker"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/didip/tollbooth"
	"github.com/didip/tollbooth/config"
	"github.com/didip/tollbooth/errors"
	"github.com/didip/tollbooth/libstring"
	giota "github.com/iotaledger/iota.lib.go"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/cors"
	uuid "github.com/satori/go.uuid"
	"github.com/uber-go/zap"
	"github.com/uber-go/zap/zwrap"
	"github.com/urfave/negroni"
)

type App struct {
	iriURI     string
	iriClient  *giota.API
	router     *httprouter.Router
	cmdLimiter *CmdLimiter
	jobMaxAge  time.Duration

	logger       zap.Logger
	incomingJobs job.JobQueue
	finishedJobs job.JobQueue
	jobStore     job.JobStore
}

type CmdLimiter struct {
	limiters map[string]*config.Limiter
	fallback *config.Limiter
}

func NewCmdLimiter(limits map[string]int64, def int64) *CmdLimiter {
	limiters := map[string]*config.Limiter{}
	for k, v := range limits {
		limiters[k] = tollbooth.NewLimiter(v, 1*time.Minute)
	}

	defLim := tollbooth.NewLimiter(def, 1*time.Minute)
	clim := &CmdLimiter{fallback: defLim, limiters: limiters}

	return clim
}

func (c *CmdLimiter) Limit(cmd string, r *http.Request) *errors.HTTPError {
	l, ok := c.limiters[cmd]
	remoteIP := libstring.RemoteIP(c.fallback.IPLookups, r)
	keys := []string{remoteIP, cmd}
	if !ok { // Use fallback if cmd was not found.
		return tollbooth.LimitByKeys(c.fallback, keys)
	}

	return tollbooth.LimitByKeys(l, keys)
}

const (
	V1BasePath = "/api/v1"
)

type ErrorResp struct {
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, code int, e ErrorResp) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	err := json.NewEncoder(w).Encode(e)
	if err != nil {
		// Oh god, bail out.
		w.Write([]byte(err.Error()))
	}
}

func (app *App) PostCommandsGetNodeInfo(w http.ResponseWriter, b []byte, _ httprouter.Params) {
	gnir := &giota.GetNodeInfoRequest{}
	err := json.Unmarshal(b, gnir)
	if err != nil {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: err.Error()})
		return
	}

	if gnir.Command != "getNodeInfo" {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid command: " + gnir.Command})
		return
	}

	gni, err := app.iriClient.GetNodeInfo()
	if err != nil {
		app.logger.Error("iri client", zap.String("callee", "GetNodeInfo"), zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: "failed to talk to IRI"})
		return
	}

	err = json.NewEncoder(w).Encode(gni)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}
}

func (app *App) PostCommandsGetTips(w http.ResponseWriter, b []byte, _ httprouter.Params) {
	gtr := &giota.GetTipsRequest{}
	err := json.Unmarshal(b, gtr)
	if err != nil {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: err.Error()})
		return
	}

	if gtr.Command != "getTips" {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid command: " + gtr.Command})
		return
	}

	gt, err := app.iriClient.GetTips()
	if err != nil {
		app.logger.Error("iri client", zap.String("callee", "GetTips"), zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: "failed to talk to IRI"})
		return
	}

	err = json.NewEncoder(w).Encode(gt)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}
}

func (app *App) PostCommandsFindTransactions(w http.ResponseWriter, b []byte, _ httprouter.Params) {
	ftr := &giota.FindTransactionsRequest{}
	err := json.Unmarshal(b, ftr)
	if err != nil {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: err.Error()})
		return
	}

	if ftr.Command != "findTransactions" {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid command: " + ftr.Command})
		return
	}

	ft, err := app.iriClient.FindTransactions(ftr)
	if err != nil {
		app.logger.Error("iri client", zap.String("callee", "FindTransactions"), zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: "failed to talk to IRI"})
		return
	}

	err = json.NewEncoder(w).Encode(ft)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}
}

func (app *App) PostCommandsGetTrytes(w http.ResponseWriter, b []byte, _ httprouter.Params) {
	gtr := &giota.GetTrytesRequest{}
	err := json.Unmarshal(b, gtr)
	if err != nil {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: err.Error()})
		return
	}

	if gtr.Command != "getTrytes" {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid command: " + gtr.Command})
		return
	}

	gt, err := app.iriClient.GetTrytes(gtr)
	if err != nil {
		app.logger.Error("iri client", zap.String("callee", "GetTrytes"), zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: "failed to talk to IRI"})
		return
	}

	err = json.NewEncoder(w).Encode(gt)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}
}

func (app *App) PostCommandsGetInclusionStates(w http.ResponseWriter, b []byte, _ httprouter.Params) {
	gisr := &giota.GetInclusionStatesRequest{}
	err := json.Unmarshal(b, gisr)
	if err != nil {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: err.Error()})
		return
	}

	if gisr.Command != "getInclusionStates" {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid command: " + gisr.Command})
		return
	}

	gis, err := app.iriClient.GetInclusionStates(gisr)
	if err != nil {
		app.logger.Error("iri client", zap.String("callee", "GetInclusionStates"), zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: "failed to talk to IRI"})
		return
	}

	err = json.NewEncoder(w).Encode(gis)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}
}

func (app *App) PostCommandsGetBalances(w http.ResponseWriter, b []byte, _ httprouter.Params) {
	gbr := &giota.GetBalancesRequest{}
	err := json.Unmarshal(b, gbr)
	if err != nil {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: err.Error()})
		return
	}

	if gbr.Command != "getBalances" {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid command: " + gbr.Command})
		return
	}

	gb, err := app.iriClient.GetBalances(gbr)
	if err != nil {
		app.logger.Error("iri client", zap.String("callee", "GetBalances"), zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: "failed to talk to IRI"})
		return
	}

	err = json.NewEncoder(w).Encode(gb)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}
}

func (app *App) PostCommandsGetTransactionsToApprove(w http.ResponseWriter, b []byte, _ httprouter.Params) {
	gttar := &giota.GetTransactionsToApproveRequest{}
	err := json.Unmarshal(b, gttar)
	if err != nil {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: err.Error()})
		return
	}

	if gttar.Command != "getTransactionsToApprove" {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid command: " + gttar.Command})
		return
	}

	gtta, err := app.iriClient.GetTransactionsToApprove(gttar)
	if err != nil {
		app.logger.Error("iri client", zap.String("callee", "GetTransactionsToApprove"), zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: "failed to talk to IRI"})
		return
	}

	err = json.NewEncoder(w).Encode(gtta)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}
}

func validTrytesSlice(ts []string) bool {
	for _, t := range ts {
		if !giota.ValidTrytes(t) {
			return false
		}
	}
	return true
}

func (app *App) PostCommandsAttachToTangle(w http.ResponseWriter, b []byte, _ httprouter.Params) {
	attr := &giota.AttachToTangleRequest{}
	err := json.Unmarshal(b, attr)
	if err != nil {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: err.Error()})
		return
	}

	if attr.Command != "attachToTangle" {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid command: " + attr.Command})
		return
	}

	if len(attr.Trytes) < 1 || !validTrytesSlice(attr.Trytes) {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid trytes"})
		return
	}

	j := job.NewIRIJob(attr.Command)
	j.AttachToTangleRequest = attr
	id, err := app.jobStore.InsertJob(j)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}

	err = app.incomingJobs.EnqueueJob(j)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}

	w.Header().Set("Link", V1BasePath+"/jobs/"+id)
	err = json.NewEncoder(w).Encode(j)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}
}

func (app *App) PostCommandsBroadcastTransactions(w http.ResponseWriter, b []byte, _ httprouter.Params) {
	btr := &giota.BroadcastTransactionsRequest{}
	err := json.Unmarshal(b, btr)
	if err != nil {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: err.Error()})
		return
	}

	if btr.Command != "broadcastTransactions" {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid command: " + btr.Command})
		return
	}

	bt, err := app.iriClient.BroadcastTransactions(btr)
	if err != nil {
		app.logger.Error("iri client", zap.String("callee", "BroadcastTransactions"), zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: "failed to talk to IRI"})
		return
	}

	err = json.NewEncoder(w).Encode(bt)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}
}

func (app *App) PostCommandsStoreTransactions(w http.ResponseWriter, b []byte, _ httprouter.Params) {
	str := &giota.StoreTransactionsRequest{}
	err := json.Unmarshal(b, str)
	if err != nil {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: err.Error()})
		return
	}

	if str.Command != "storeTransactions" {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid command: " + str.Command})
		return
	}

	st, err := app.iriClient.StoreTransactions(str)
	if err != nil {
		app.logger.Error("iri client", zap.String("callee", "StoreTransactions"), zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: "failed to talk to IRI"})
		return
	}

	err = json.NewEncoder(w).Encode(st)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}
}

type PostCommand struct {
	Command string `json:"command"`
}

func (app *App) PostCommands(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	lr := io.LimitReader(r.Body, 8388608) // 2^23 bytes
	b, err := ioutil.ReadAll(lr)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}

	cmd := &PostCommand{}
	err = json.Unmarshal(b, cmd)
	if err != nil {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: err.Error()})
		return
	}

	auth := r.Context().Value("authorization")
	if auth == nil || !auth.(bool) { // If we're not authenticated, then we're subjected to rate limiting.
		app.logger.Debug("not authed")
		limitReached := app.cmdLimiter.Limit(cmd.Command, r)
		if limitReached != nil {
			writeError(w, limitReached.StatusCode, ErrorResp{Message: limitReached.Message})
			return
		}
	}

	switch cmd.Command {
	default:
		writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid command: " + cmd.Command})
		return
	case "getNodeInfo":
		app.PostCommandsGetNodeInfo(w, b, nil)
	case "getTips":
		app.PostCommandsGetTips(w, b, nil)
	case "findTransactions":
		app.PostCommandsFindTransactions(w, b, nil)
	case "getTrytes":
		app.PostCommandsGetTrytes(w, b, nil)
	case "getInclusionStates":
		app.PostCommandsGetInclusionStates(w, b, nil)
	case "getBalances":
		app.PostCommandsGetBalances(w, b, nil)
	case "getTransactionsToApprove":
		app.PostCommandsGetTransactionsToApprove(w, b, nil)
	case "broadcastTransactions":
		app.PostCommandsBroadcastTransactions(w, b, nil)
	case "storeTransactions":
		app.PostCommandsStoreTransactions(w, b, nil)
	case "attachToTangle":
		app.PostCommandsAttachToTangle(w, b, nil)
	}
}

func (app *App) GetJobsID(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := uuid.FromStringOrNil(ps.ByName("id"))
	if id == uuid.Nil {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid id"})
		return
	}

	j, err := app.jobStore.SelectJob(id.String())
	if err == job.ErrJobNotFound {
		writeError(w, http.StatusNotFound, ErrorResp{Message: err.Error()})
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}

	err = json.NewEncoder(w).Encode(j)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}
}

func (app *App) PullFinishedJobs() {
	for {
		j, err := app.finishedJobs.DequeueJob()
		if err != nil || j == nil {
			app.logger.Debug("got no new finished jobs")
			continue
		}
		app.logger.Debug("got new finished job", zap.Object("job", j))
		_, err = app.jobStore.UpdateJob(j.ID, j)
		if err != nil {
			app.logger.Error("updating job store", zap.Error(err))
		}
	}
}

func (app *App) TimeoutJobs() {
	for {
		app.jobStore.TimeoutJobs(app.jobMaxAge)
		time.Sleep(app.jobMaxAge)
	}
}

var (
	awsAccessKeyID     = os.Getenv("AWS_ACCESS_KEY_ID")
	awsSecretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	awsRegion          = os.Getenv("AWS_REGION")
	incomingQueueName  = os.Getenv("INCOMING_QUEUE_NAME")
	finishedQueueName  = os.Getenv("FINISHED_QUEUE_NAME")
)

func main() {
	app := &App{}
	app.iriURI = "http://localhost:14265/"
	if os.Getenv("IRI_URI") != "" {
		app.iriURI = os.Getenv("IRI_URI")
	}

	if os.Getenv("DEBUG") == "1" {
		app.logger = zap.New(zap.NewJSONEncoder(), zap.DebugLevel)
	} else {
		app.logger = zap.New(zap.NewJSONEncoder())
	}

	// Transport for the client that talks to the IRI instance(s).
	tr := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	client := &http.Client{
		Transport: tr,
	}

	ic, err := giota.NewAPI(app.iriURI, client)
	if err != nil {
		app.logger.Fatal("initializing IRI API client", zap.Error(err))
	}
	app.iriClient = ic

	if awsAccessKeyID == "" {
		app.logger.Fatal("$AWS_ACCESS_KEY_ID not set")
	}
	if awsSecretAccessKey == "" {
		app.logger.Fatal("$AWS_SECRET_ACCESS_KEY not set")
	}
	if awsRegion == "" {
		app.logger.Fatal("$AWS_REGION not set")
	}
	if incomingQueueName == "" {
		app.logger.Fatal("$INCOMING_QUEUE_NAME not set")
	}
	if finishedQueueName == "" {
		app.logger.Fatal("$FINISHED_QUEUE_NAME not set")
	}

	cred := credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, "")
	sess := session.New(
		&aws.Config{
			Credentials: cred,
			Region:      &awsRegion,
		},
	)

	// XXX: make more resilient, ie initialize with or without err
	// 			and retry later if the QueueUrl is empty.
	inc, err := job.NewSQSQueue(sess, incomingQueueName)
	if err != nil {
		app.logger.Fatal("incoming job queue", zap.Error(err))
	}
	app.incomingJobs = inc

	fin, err := job.NewSQSQueue(sess, finishedQueueName)
	if err != nil {
		app.logger.Fatal("finished job queue", zap.Error(err))
	}
	app.finishedJobs = fin

	jobMaxAge, err := time.ParseDuration(os.Getenv("JOB_MAX_AGE"))
	if err == nil {
		app.jobMaxAge = jobMaxAge
	} else {
		app.jobMaxAge = 5 * time.Minute
	}

	mgURI := os.Getenv("MONGO_URI")
	var js job.JobStore
	if mgURI != "" {
		js, err = job.NewMongoStore(mgURI, "sandbox")
		if err != nil {
			app.logger.Fatal("init job store", zap.Error(err))
		}
	} else {
		js = job.NewMemoryStore()
	}
	app.jobStore = js

	r := httprouter.New()
	r.POST("/api/v1/commands", app.PostCommands)
	r.GET("/api/v1/jobs/:id", app.GetJobsID)

	recovLogger, err := zwrap.Standardize(app.logger, zap.ErrorLevel)
	if err != nil {
		app.logger.Fatal("standardize zap logger", zap.Error(err))
	}

	// XXX: Make this configurable.
	app.cmdLimiter = NewCmdLimiter(map[string]int64{"attachToTangle": 1}, 5)

	n := negroni.New()

	recov := negroni.NewRecovery()
	recov.PrintStack = false
	recov.Logger = recovLogger
	n.Use(recov)
	n.Use(NewLoggerMiddleware())
	n.Use(ContentTypeEnforcer("application/json", "application/x-www-form-urlencoded"))

	var as AuthStore
	if mgURI != "" {
		as, err = NewMongoStore(mgURI, "sandbox")
		if err != nil {
			app.logger.Fatal("init auth store", zap.Error(err))
		}
	} else {
		as, err = NewDummyStore()
		if err != nil {
			app.logger.Fatal("init auth store", zap.Error(err))
		}
	}

	auth := NewAuthMiddleware(as)
	n.Use(auth)

	hardLimit, err := strconv.ParseInt(os.Getenv("REQUESTS_PER_MINUTE"), 10, 64)
	if err == nil && hardLimit > 0 {
		limiter := tollbooth.NewLimiter(hardLimit, time.Minute)
		n.Use(LimitHandler(limiter))
	}

	n.Use(cors.Default())
	n.UseHandler(r)

	listenAddr := os.Getenv("LISTEN_ADDRESS")
	if listenAddr == "" {
		listenAddr = "0.0.0.0:8080"
	}

	srv := &http.Server{
		Handler:      n,
		Addr:         listenAddr,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go app.PullFinishedJobs()
	go app.TimeoutJobs()
	app.logger.Info("starting listener")
	app.logger.Fatal("ListenAndServe", zap.Error(srv.ListenAndServe()))
}
