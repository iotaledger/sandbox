package main

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/iotaledger/giota"
	"github.com/iotaledger/sandbox/auth"
	"github.com/iotaledger/sandbox/job"

	"github.com/DataDog/datadog-go/statsd"
	"go.uber.org/zap"

	"cloud.google.com/go/pubsub"
	"github.com/didip/tollbooth"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/cors"
	uuid "github.com/satori/go.uuid"
	"github.com/urfave/negroni"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

type App struct {
	iriClient  *giota.API
	router     *httprouter.Router
	cmdLimiter *CmdLimiter
	jobMaxAge  time.Duration

	logger *zap.Logger
	stats  *statsd.Client

	incomingJobsTopic        *pubsub.Topic
	finishedJobsSubscription *pubsub.Subscription
	jobStore                 job.JobStore
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

	gt, err := app.iriClient.GetTrytes(gtr.Hashes)
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

	gis, err := app.iriClient.GetInclusionStates(gisr.Transactions, gisr.Tips)
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

	gb, err := app.iriClient.GetBalances(gbr.Addresses, gbr.Threshold)
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

	gtta, err := app.iriClient.GetTransactionsToApprove(gttar.Depth)
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

func validTrytesSlice(ts []giota.Trytes) bool {
	for _, t := range ts {
		if t.IsValid() != nil {
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

	if len(attr.Trytes) < 1 {
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

	jb, err := json.Marshal(j)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}

	ctx := context.Background()
	r := app.incomingJobsTopic.Publish(ctx, &pubsub.Message{
		Data: jb,
	})

	go func() {
		_, err := r.Get(ctx)
		if err != nil {
			app.logger.Error("publish pubsub message", zap.Error(err))
			j.Error = &job.JobError{Message: "unable to add job to queue"}
			j.Status = job.JobStatusFailed
			_, err := app.jobStore.UpdateJob(id, j)
			if err != nil {
				app.logger.Error("update jobStore", zap.Error(err))
			}
		}
	}()

	w.WriteHeader(http.StatusAccepted)
	w.Header().Set("Link", V1BasePath+"/jobs/"+id)
	err = json.NewEncoder(w).Encode(j)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}

	return
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

	err = app.iriClient.BroadcastTransactions(btr.Trytes)
	if err != nil {
		app.logger.Error("iri client", zap.String("callee", "BroadcastTransactions"), zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: "failed to talk to IRI"})
		return
	}

	err = json.NewEncoder(w).Encode(struct{}{})
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

	err = app.iriClient.StoreTransactions(str.Trytes)
	if err != nil {
		app.logger.Error("iri client", zap.String("callee", "StoreTransactions"), zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: "failed to talk to IRI"})
		return
	}

	err = json.NewEncoder(w).Encode(struct{}{})
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
		limitReached := app.cmdLimiter.Limit(cmd.Command, r)
		if limitReached != nil {
			writeError(w, limitReached.StatusCode, ErrorResp{Message: limitReached.Message})
			return
		}
	}

	switch cmd.Command {
	default:
		app.stats.Incr("request.command.unknown", nil, 1)
		writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid command: " + cmd.Command})
		return
	case "getNodeInfo":
		app.stats.Incr("request.command."+cmd.Command, nil, 1)
		app.PostCommandsGetNodeInfo(w, b, nil)
	case "getTips":
		app.stats.Incr("request.command."+cmd.Command, nil, 1)
		app.PostCommandsGetTips(w, b, nil)
	case "findTransactions":
		app.stats.Incr("request.command."+cmd.Command, nil, 1)
		app.PostCommandsFindTransactions(w, b, nil)
	case "getTrytes":
		app.stats.Incr("request.command."+cmd.Command, nil, 1)
		app.PostCommandsGetTrytes(w, b, nil)
	case "getInclusionStates":
		app.stats.Incr("request.command."+cmd.Command, nil, 1)
		app.PostCommandsGetInclusionStates(w, b, nil)
	case "getBalances":
		app.stats.Incr("request.command."+cmd.Command, nil, 1)
		app.PostCommandsGetBalances(w, b, nil)
	case "getTransactionsToApprove":
		app.stats.Incr("request.command."+cmd.Command, nil, 1)
		app.PostCommandsGetTransactionsToApprove(w, b, nil)
	case "broadcastTransactions":
		app.stats.Incr("request.command."+cmd.Command, nil, 1)
		app.PostCommandsBroadcastTransactions(w, b, nil)
	case "storeTransactions":
		app.stats.Incr("request.command."+cmd.Command, nil, 1)
		app.PostCommandsStoreTransactions(w, b, nil)
	case "attachToTangle":
		app.stats.Incr("request.command."+cmd.Command, nil, 1)
		app.PostCommandsAttachToTangle(w, b, nil)
	}
}

func (app *App) GetJobsID(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := uuid.FromStringOrNil(ps.ByName("id"))
	if id == uuid.Nil {
		writeError(w, http.StatusBadRequest, ErrorResp{Message: "invalid id"})
		return
	}

	app.stats.Incr("request.jobs", nil, 1)

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

type APIStatus struct {
	IRIReachable bool `json:"iriReachable"`
	JobStats     struct {
		Since       int64   `json:"since"`
		Processed   int64   `json:"processed"`
		FailureRate float64 `json:"failureRate"`
	} `json:"jobStats"`
}

func (app *App) GetAPIStatus(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	app.stats.Incr("request.status", nil, 1)

	_, err := app.iriClient.GetNodeInfo()
	s := &APIStatus{}
	t := time.Now().Add(-1 * time.Hour)
	count, rate := app.jobStore.JobFailureRate(-1 * time.Hour)
	s.JobStats.Since = t.Unix()
	s.JobStats.Processed = count
	s.JobStats.FailureRate = rate
	s.IRIReachable = err == nil

	err = json.NewEncoder(w).Encode(s)
	if err != nil {
		writeError(w, http.StatusInternalServerError, ErrorResp{Message: err.Error()})
		return
	}
}

// PullFinishedJobs sets up the receiver function for the finished jobs pubsub
// subscription.
func (app *App) PullFinishedJobs() error {
	ctx := context.Background()
	err := app.finishedJobsSubscription.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		app.logger.Debug("got new finished job")
		j := &job.IRIJob{}
		err := json.Unmarshal(m.Data, j)
		if err != nil {
			app.logger.Error("unmarshal finished job", zap.Error(err))
			m.Ack()
			return
		}

		_, err = app.jobStore.UpdateJob(j.ID, j)
		if err != nil {
			app.logger.Error("updating job store with finished job", zap.Error(err))
		}

		m.Ack()
	})
	app.logger.Info("finished job receiver started", zap.Error(err))
	return err
}

func (app *App) TimeoutJobs() {
	t := time.NewTicker(app.jobMaxAge)
	for _ = range t.C {
		app.jobStore.TimeoutJobs(app.jobMaxAge)
	}
}

// This gets the pubsub topics/subscriptions into the proper state, i.e. checks
// if all of them are available and if not creates them.
func (app *App) initPubSub(credPath string) {
	ctx := context.Background()
	var psClient *pubsub.Client

	if credPath != "" {
		pc, err := pubsub.NewClient(ctx, googleProjectID, option.WithServiceAccountFile(credPath))
		if err != nil {
			app.logger.Fatal("pubsub client", zap.Error(err))
		}
		psClient = pc
	} else {
		pc, err := pubsub.NewClient(ctx, googleProjectID)
		if err != nil {
			app.logger.Fatal("pubsub client", zap.Error(err))
		}
		psClient = pc
	}

	app.incomingJobsTopic = psClient.Topic(incomingJobsTopicName)
	ok, err := app.incomingJobsTopic.Exists(ctx)
	if err != nil {
		app.logger.Fatal("pubsub topic", zap.Error(err), zap.String("name", incomingJobsTopicName))
	} else if !ok {
		t, err := psClient.CreateTopic(ctx, incomingJobsTopicName)
		if err != nil {
			app.logger.Fatal("pubsub topic", zap.Error(err), zap.String("name", incomingJobsTopicName))
		}
		app.incomingJobsTopic = t
	}

	incSub := psClient.Subscription(incomingJobsSubscriptionName)
	ok, err = incSub.Exists(ctx)
	if err != nil {
		app.logger.Fatal("pubsub subscription", zap.Error(err), zap.String("name", incomingJobsSubscriptionName))
	} else if !ok {
		subConfig := pubsub.SubscriptionConfig{
			Topic:       app.incomingJobsTopic,
			AckDeadline: 120 * time.Second,
		}
		_, err := psClient.CreateSubscription(ctx, incomingJobsSubscriptionName, subConfig)
		if err != nil {
			app.logger.Fatal("pubsub subscription", zap.Error(err), zap.String("name", incomingJobsSubscriptionName))
		}
	}

	finTopic := psClient.Topic(finishedJobsTopicName)
	ok, err = finTopic.Exists(ctx)
	if err != nil {
		app.logger.Fatal("pubsub topic", zap.Error(err), zap.String("name", finishedJobsTopicName))
	} else if !ok {
		_, err := psClient.CreateTopic(ctx, finishedJobsTopicName)
		if err != nil {
			app.logger.Fatal("pubsub topic", zap.Error(err), zap.String("name", finishedJobsTopicName))
		}
	}

	app.finishedJobsSubscription = psClient.Subscription(finishedJobsSubscriptionName)
	ok, err = app.finishedJobsSubscription.Exists(ctx)
	if err != nil {
		app.logger.Fatal("pubsub subscription", zap.Error(err), zap.String("name", finishedJobsSubscriptionName))
	} else if !ok {
		subConfig := pubsub.SubscriptionConfig{
			Topic:       finTopic,
			AckDeadline: 120 * time.Second,
		}
		s, err := psClient.CreateSubscription(ctx, finishedJobsSubscriptionName, subConfig)
		if err != nil {
			app.logger.Fatal("pubsub subscription", zap.Error(err), zap.String("name", finishedJobsSubscriptionName))
		}
		app.finishedJobsSubscription = s
	}
}

var (
	incomingJobsTopicName        = os.Getenv("INCOMING_JOBS_TOPIC")
	finishedJobsTopicName        = os.Getenv("FINISHED_JOBS_TOPIC")
	incomingJobsSubscriptionName = os.Getenv("INCOMING_JOBS_SUBSCRIPTION")
	finishedJobsSubscriptionName = os.Getenv("FINISHED_JOBS_SUBSCRIPTION")
	googleProjectID              = os.Getenv("GOOGLE_PROJECT_ID")
)

func main() {
	app := &App{}

	if os.Getenv("DEBUG") == "1" {
		logger, err := zap.NewDevelopment()
		if err != nil {
			log.Fatalf("failed to initialize logger: %s", err)
		}
		app.logger = logger
	} else {
		logger, err := zap.NewProduction()
		if err != nil {
			log.Fatalf("failed to initialize logger: %s", err)
		}
		app.logger = logger
	}

	sh := os.Getenv("STATSD_URI")
	if sh == "" {
		sh = "127.0.0.1:8125"
	}
	c, err := statsd.NewBuffered(sh, 100)
	if err != nil {
		app.logger.Fatal("failed to create statsink", zap.Error(err))
	}
	app.stats = c

	// Transport for the client that talks to the IRI instance(s).
	tr := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
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

	app.iriClient = giota.NewAPI(os.Getenv("IRI_URI"), client)

	credPath := os.Getenv("GOOGLE_CREDENTIALS_FILE")
	app.initPubSub(credPath)

	jobMaxAge, err := time.ParseDuration(os.Getenv("JOB_MAX_AGE"))
	if err == nil {
		app.jobMaxAge = jobMaxAge
	} else {
		app.jobMaxAge = 5 * time.Minute
	}

	js, err := job.NewGCloudDataStore(googleProjectID, credPath)
	if err != nil {
		app.logger.Fatal("init job store", zap.Error(err))
	}
	app.jobStore = js

	r := httprouter.New()
	r.POST("/api/v1/commands", app.PostCommands)
	r.GET("/api/v1/jobs/:id", app.GetJobsID)
	r.GET("/api/v1/status", app.GetAPIStatus)

	// XXX: Make this configurable.
	app.cmdLimiter = NewCmdLimiter(map[string]int64{"attachToTangle": 1}, 5)

	n := negroni.New()

	recov := negroni.NewRecovery()
	recov.PrintStack = false
	n.Use(recov)
	lm, err := NewLoggerMiddleware(app.logger)
	if err != nil {
		app.logger.Fatal("init logger middleware", zap.Error(err))
	}
	n.Use(lm)
	n.Use(ContentTypeEnforcer("application/json", "application/x-www-form-urlencoded"))

	as, err := auth.NewGCloudDataStore(googleProjectID, credPath)
	if err != nil {
		app.logger.Fatal("init auth store", zap.Error(err))
	}

	auth := NewAuthMiddleware(as)
	n.Use(auth)

	hardLimit, err := strconv.ParseInt(os.Getenv("REQUESTS_PER_MINUTE"), 10, 64)
	if err == nil && hardLimit > 0 {
		limiter := tollbooth.NewLimiter(hardLimit, time.Minute, nil)
		n.Use(LimitHandler(limiter))
	}

	co := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"HEAD", "GET", "POST", "PUT", "PATCH", "DELETE"},
		AllowedHeaders:   []string{"Authorization", "Content-Type", "X-IOTA-API-Version"},
		AllowCredentials: true,
	})
	n.Use(co)
	n.UseHandler(r)

	listenAddr := os.Getenv("LISTEN_ADDRESS")
	if listenAddr == "" {
		listenAddr = "0.0.0.0:8080"
	}

	srv := &http.Server{
		Handler:      n,
		Addr:         listenAddr,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go app.PullFinishedJobs()
	go app.TimeoutJobs()
	app.logger.Info("starting listener")
	app.logger.Fatal("ListenAndServe", zap.Error(srv.ListenAndServe()))
}
