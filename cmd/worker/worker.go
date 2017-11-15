package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/iotaledger/sandbox/job"

	"cloud.google.com/go/pubsub"
	"github.com/iotaledger/giota"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

func (app *App) failJob(j *job.IRIJob, errMsg string) {
	j.Error = &job.JobError{Message: errMsg}
	j.Status = job.JobStatusFailed
	j.FinishedAt = time.Now().Unix()

	jb, err := json.Marshal(j)
	if err != nil {
		app.logger.Error("marshal job", zap.Error(err))
		return
	}

	ctx := context.Background()
	r := app.finishedJobsTopic.Publish(ctx, &pubsub.Message{
		Data: jb,
	})

	go func() {
		_, err := r.Get(ctx)
		if err != nil {
			app.logger.Error("publish pubsub message", zap.Error(err))
		}
	}()
	app.logger.Debug("failJob enqueued", zap.String("errMsg", errMsg), zap.Any("job", j))
}

func (app *App) processTransactions(ctx context.Context, attachReq *giota.AttachToTangleRequest) ([]giota.Transaction, error) {
	ctx.Err()

	outTxs := []giota.Transaction{}
	var prevTxHash *giota.Trytes = nil

	for _, ts := range attachReq.Trytes {
		trits := ts.Trytes().Trits()
		if prevTxHash == nil {
			copy(trits[giota.TrunkTransactionTrinaryOffset:giota.TrunkTransactionTrinaryOffset+giota.TrunkTransactionTrinarySize], attachReq.TrunkTransaction.Trits())
			copy(trits[giota.BranchTransactionTrinaryOffset:giota.BranchTransactionTrinaryOffset+giota.BranchTransactionTrinarySize], attachReq.BranchTransaction.Trits())
		} else {
			copy(trits[giota.TrunkTransactionTrinaryOffset:giota.TrunkTransactionTrinaryOffset+giota.TrunkTransactionTrinarySize], prevTxHash.Trits())
			copy(trits[giota.BranchTransactionTrinaryOffset:giota.BranchTransactionTrinaryOffset+giota.BranchTransactionTrinarySize], attachReq.TrunkTransaction.Trits())
		}

		s := time.Now()

		cmd := exec.CommandContext(ctx, app.ccurlPath, strconv.Itoa(app.minWeightMagnitude), string(trits.Trytes()))
		out, err := cmd.Output()
		if err != nil {
			app.logger.Error("ccurl", zap.Error(err))
			if err := ctx.Err(); err == context.DeadlineExceeded {
				return nil, fmt.Errorf("job exceeded time quota")
			}
			return nil, err
		}

		outS := string(out)
		outTx, err := giota.NewTransaction(giota.Trytes(outS))
		if err != nil {
			return outTxs, fmt.Errorf("invalid transaction after ccurl: %s", err)
		}

		h := outTx.Hash()
		prevTxHash = &h
		app.logger.Info("runtime", zap.Duration("ccurl", time.Since(s)))
		outTxs = append(outTxs, *outTx)
	}

	return outTxs, nil
}

func (app *App) HandleAttachToTangle(ctx context.Context, j *job.IRIJob) error {
	if j.AttachToTangleRequest == nil {
		return fmt.Errorf("no attachToTangleRequest supplied")
	}

	if len(j.AttachToTangleRequest.Trytes) < 1 {
		return fmt.Errorf("no trytes supplied for job")
	}

	outTxs, err := app.processTransactions(ctx, j.AttachToTangleRequest)
	if err != nil {
		return err
	}

	j.AttachToTangleRespose = &giota.AttachToTangleResponse{Trytes: outTxs}
	j.Status = job.JobStatusFinished
	j.FinishedAt = time.Now().Unix()

	jb, err := json.Marshal(j)
	if err != nil {
		return err
	}

	ctx = context.Background()
	r := app.finishedJobsTopic.Publish(ctx, &pubsub.Message{
		Data: jb,
	})

	go func() {
		_, err := r.Get(ctx)
		if err != nil {
			app.logger.Error("publish pubsub message", zap.Error(err))
			return
		}
		app.logger.Debug("published finished job")
	}()

	return nil
}

func (app *App) Worker() error {
	ctx := context.Background()
	err := app.incomingJobsSubscription.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		app.logger.Debug("got new incoming job")
		j := &job.IRIJob{}
		err := json.Unmarshal(m.Data, j)
		if err != nil {
			app.logger.Error("unmarshal job", zap.Error(err))
		}

		j.StartedAt = time.Now().Unix()
		//app.logger.Debug("new job", zap.Any("job", j))
		switch j.Command {
		default:
			app.failJob(j, fmt.Sprintf("unknown command %q", j.Command))
		case "attachToTangle":
			ctx := context.Background()
			err := app.HandleAttachToTangle(ctx, j)
			if err != nil {
				app.failJob(j, err.Error())
				app.logger.Error("attach to tangle", zap.Error(err))
				return
			}
		}
		m.Ack()
	})
	return err
}

type App struct {
	ccurlPath          string
	ccurlTimeout       time.Duration
	minWeightMagnitude int

	logger                   *zap.Logger
	finishedJobsTopic        *pubsub.Topic
	incomingJobsSubscription *pubsub.Subscription
}

var (
	finishedJobsTopicName        = os.Getenv("FINISHED_JOBS_TOPIC")
	incomingJobsSubscriptionName = os.Getenv("INCOMING_JOBS_SUBSCRIPTION")
	googleProjectID              = os.Getenv("GOOGLE_PROJECT_ID")
)

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

	app.finishedJobsTopic = psClient.Topic(finishedJobsTopicName)
	ok, err := app.finishedJobsTopic.Exists(ctx)
	if err != nil {
		app.logger.Fatal("pubsub topic", zap.Error(err), zap.String("name", finishedJobsTopicName))
	} else if !ok {
		app.logger.Fatal("pubsub topic does not exist", zap.String("name", finishedJobsTopicName))
	}

	app.incomingJobsSubscription = psClient.Subscription(incomingJobsSubscriptionName)
	ok, err = app.incomingJobsSubscription.Exists(ctx)
	if err != nil {
		app.logger.Fatal("pubsub subscription", zap.Error(err), zap.String("name", incomingJobsSubscriptionName))
	} else if !ok {
		app.logger.Fatal("pubsub subscription does not exist", zap.String("name", incomingJobsSubscriptionName))
	}
}

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

	// First setup ccurl
	app.ccurlPath = os.Getenv("CCURL_PATH")
	if app.ccurlPath == "" {
		app.logger.Fatal("$CCURL_PATH not set")
	}

	mwm, err := strconv.Atoi(os.Getenv("MIN_WEIGHT_MAGNITUDE"))
	if err != nil {
		app.logger.Fatal("$MIN_WEIGHT_MAGNITUDE is not a valid number")
	}
	app.minWeightMagnitude = mwm

	app.ccurlTimeout = 120 * time.Second
	to, err := strconv.Atoi(os.Getenv("CCURL_TIMEOUT"))
	if err == nil {
		app.ccurlTimeout = time.Duration(to) * time.Second
	}

	app.initPubSub(os.Getenv("GOOGLE_CREDENTIALS_FILE"))

	// TODO: add graceful shutdown
	app.logger.Info("starting worker")
	app.Worker()
}
