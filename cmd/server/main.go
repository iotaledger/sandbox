package main

import (
	"encoding/json"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/iotaledger/sandbox/job"

	//"github.com/eapache/go-resiliency/breaker"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/didip/tollbooth"
	giota "github.com/iotaledger/iota.lib.go"
	"github.com/julienschmidt/httprouter"
	uuid "github.com/satori/go.uuid"
	"github.com/uber-go/zap"
	"github.com/urfave/negroni"
)

type App struct {
	iriURI    string
	iriClient *giota.API
	router    *httprouter.Router

	logger       zap.Logger
	incomingJobs job.JobQueue
	finishedJobs job.JobQueue
	jobStore     job.JobStore
}

type ErrorResp struct {
	Message string
}

func (app *App) writeError(w http.ResponseWriter, code int, e ErrorResp) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	err := json.NewEncoder(w).Encode(e)
	if err != nil {
		// Oh god, bail out.
		w.Write([]byte(err.Error()))
	}
}

func (app *App) PostCommandsGetNodeInfo(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	lr := http.MaxBytesReader(w, r.Body, 8388608) // 2^23 bytes
	gnir := &giota.GetNodeInfoRequest{}
	err := json.NewDecoder(lr).Decode(gnir)
	if err != nil {
		app.writeError(w, http.StatusBadRequest, ErrorResp{Message: err.Error()})
		return
	}

	if gnir.Command != "getNodeInfo" {
		app.writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid command: " + gnir.Command})
		return
	}

	gni, err := app.iriClient.GetNodeInfo()
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}

	err = json.NewEncoder(w).Encode(gni)
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}
}

func (app *App) PostCommandsGetTips(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	lr := http.MaxBytesReader(w, r.Body, 8388608) // 2^23 bytes
	gtr := &giota.GetTipsRequest{}
	err := json.NewDecoder(lr).Decode(gtr)
	if err != nil {
		app.writeError(w, http.StatusBadRequest, ErrorResp{Message: err.Error()})
		return
	}

	if gtr.Command != "getTips" {
		app.writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid command: " + gtr.Command})
		return
	}

	gt, err := app.iriClient.GetTips()
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}

	err = json.NewEncoder(w).Encode(gt)
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}
}

func (app *App) PostCommandsFindTransactions(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	lr := http.MaxBytesReader(w, r.Body, 8388608) // 2^23 bytes
	ftr := &giota.FindTransactionsRequest{}
	err := json.NewDecoder(lr).Decode(ftr)
	if err != nil {
		app.writeError(w, http.StatusBadRequest, ErrorResp{Message: err.Error()})
		return
	}

	if ftr.Command != "findTransactions" {
		app.writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid command: " + ftr.Command})
		return
	}

	ft, err := app.iriClient.FindTransactions(ftr)
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}

	err = json.NewEncoder(w).Encode(ft)
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}
}

func (app *App) PostCommandsGetTrytes(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	lr := http.MaxBytesReader(w, r.Body, 8388608) // 2^23 bytes
	gtr := &giota.GetTrytesRequest{}
	err := json.NewDecoder(lr).Decode(gtr)
	if err != nil {
		app.writeError(w, http.StatusBadRequest, ErrorResp{Message: err.Error()})
		return
	}

	if gtr.Command != "getTrytes" {
		app.writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid command: " + gtr.Command})
		return
	}

	gt, err := app.iriClient.GetTrytes(gtr)
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}

	err = json.NewEncoder(w).Encode(gt)
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}
}

func (app *App) PostCommandsGetInclusionStates(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	lr := http.MaxBytesReader(w, r.Body, 8388608) // 2^23 bytes
	gisr := &giota.GetInclusionStatesRequest{}
	err := json.NewDecoder(lr).Decode(gisr)
	if err != nil {
		app.writeError(w, http.StatusBadRequest, ErrorResp{Message: err.Error()})
		return
	}

	if gisr.Command != "getInclusionStates" {
		app.writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid command: " + gisr.Command})
		return
	}

	gis, err := app.iriClient.GetInclusionStates(gisr)
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}

	err = json.NewEncoder(w).Encode(gis)
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}
}

func (app *App) PostCommandsGetBalances(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	lr := http.MaxBytesReader(w, r.Body, 8388608) // 2^23 bytes
	gbr := &giota.GetBalancesRequest{}
	err := json.NewDecoder(lr).Decode(gbr)
	if err != nil {
		app.writeError(w, http.StatusBadRequest, ErrorResp{Message: err.Error()})
		return
	}

	if gbr.Command != "getBalances" {
		app.writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid command: " + gbr.Command})
		return
	}

	gb, err := app.iriClient.GetBalances(gbr)
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}

	err = json.NewEncoder(w).Encode(gb)
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}
}

func (app *App) PostCommandsGetTransactionsToApprove(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	lr := http.MaxBytesReader(w, r.Body, 8388608) // 2^23 bytes
	gttar := &giota.GetTransactionsToApproveRequest{}
	err := json.NewDecoder(lr).Decode(gttar)
	if err != nil {
		app.writeError(w, http.StatusBadRequest, ErrorResp{Message: err.Error()})
		return
	}

	if gttar.Command != "getTransactionsToApprove" {
		app.writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid command: " + gttar.Command})
		return
	}

	gtta, err := app.iriClient.GetTransactionsToApprove(gttar)
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}

	err = json.NewEncoder(w).Encode(gtta)
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
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

func (app *App) PostCommandsAttachToTangle(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	lr := http.MaxBytesReader(w, r.Body, 8388608) // 2^23 bytes
	attr := &giota.AttachToTangleRequest{}
	err := json.NewDecoder(lr).Decode(attr)
	if err != nil {
		app.writeError(w, http.StatusBadRequest, ErrorResp{Message: err.Error()})
		return
	}

	if attr.Command != "attachToTangle" {
		app.writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid command: " + attr.Command})
		return
	}

	if len(attr.Trytes) < 1 || !validTrytesSlice(attr.Trytes) {
		app.writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid trytes"})
		return
	}

	j := job.NewIRIJob(attr.Command)
	j.AttachToTangleRequest = attr
	id, err := app.jobStore.InsertJob(j)
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}

	err = app.incomingJobs.EnqueueJob(j)
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}

	w.Header().Set("Link", "/jobs/"+id.String())
	err = json.NewEncoder(w).Encode(j)
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}
}

func (app *App) PostCommandsBroadcastTransactions(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	lr := http.MaxBytesReader(w, r.Body, 8388608) // 2^23 bytes
	btr := &giota.BroadcastTransactionsRequest{}
	err := json.NewDecoder(lr).Decode(btr)
	if err != nil {
		app.writeError(w, http.StatusBadRequest, ErrorResp{Message: err.Error()})
		return
	}

	if btr.Command != "broadcastTransactions" {
		app.writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid command: " + btr.Command})
		return
	}

	bt, err := app.iriClient.BroadcastTransactions(btr)
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}

	err = json.NewEncoder(w).Encode(bt)
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}
}

func (app *App) PostCommandsStoreTransactions(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	lr := http.MaxBytesReader(w, r.Body, 8388608) // 2^23 bytes
	str := &giota.StoreTransactionsRequest{}
	err := json.NewDecoder(lr).Decode(str)
	if err != nil {
		app.writeError(w, http.StatusBadRequest, ErrorResp{Message: err.Error()})
		return
	}

	if str.Command != "storeTransactions" {
		app.writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid command: " + str.Command})
		return
	}

	st, err := app.iriClient.StoreTransactions(str)
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}

	err = json.NewEncoder(w).Encode(st)
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}
}

func (app *App) GetJobsID(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := uuid.FromStringOrNil(ps.ByName("id"))
	if id == uuid.Nil {
		app.writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid id"})

	}

	job, err := app.jobStore.SelectJob(id)
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}

	err = json.NewEncoder(w).Encode(job)
	if err != nil {
		app.writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
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
	c := &http.Client{
		Transport: tr,
	}

	ic, err := giota.NewAPI(app.iriURI, c)
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

	s := job.NewMemoryStore()
	app.jobStore = s

	r := httprouter.New()
	r.POST("/commands/getNodeInfo", app.PostCommandsGetNodeInfo)
	r.POST("/commands/getTips", app.PostCommandsGetTips)
	r.POST("/commands/findTransactions", app.PostCommandsFindTransactions)
	r.POST("/commands/getTrytes", app.PostCommandsGetTrytes)
	r.POST("/commands/getInclusionStates", app.PostCommandsGetInclusionStates)
	r.POST("/commands/getBalances", app.PostCommandsGetBalances)
	r.POST("/commands/getTransactionsToApprove", app.PostCommandsGetTransactionsToApprove)
	r.POST("/commands/broadcastTransactions", app.PostCommandsBroadcastTransactions)
	r.POST("/commands/storeTransactions", app.PostCommandsStoreTransactions)

	r.POST("/commands/attachToTangle", app.PostCommandsAttachToTangle)

	r.GET("/jobs/:id", app.GetJobsID)

	limiter := tollbooth.NewLimiter(60, time.Minute)

	n := negroni.New()
	n.Use(negroni.NewRecovery())
	n.Use(NewLoggerMiddleware())
	n.Use(ContentTypeEnforcer("application/json", "application/x-www-form-urlencoded"))
	n.Use(LimitHandler(limiter))
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

	app.logger.Info("starting listener")
	app.logger.Fatal("ListenAndServe", zap.Error(srv.ListenAndServe()))
}
