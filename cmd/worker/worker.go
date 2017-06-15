package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/iotaledger/sandbox/job"

	"cloud.google.com/go/pubsub"
	giota "github.com/iotaledger/iota.lib.go"
	"go.uber.org/zap"
	"golang.org/x/net/context"
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

		app.logger.Debug("writing pipe", zap.String("trytes", string(trits.Trytes())), zap.Int("len", len(trits.Trytes())))
		_, err := app.ccurld.Write(string(trits.Trytes()))
		if err != nil {
			return outTxs, err
		}

		outS, err := app.ccurld.Read()
		if err != nil {
			return outTxs, err
		}
		app.logger.Debug("read from pipe", zap.String("out", outS))

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
	logger                   *zap.Logger
	finishedJobsTopic        *pubsub.Topic
	incomingJobsSubscription *pubsub.Subscription

	ccurld *ccurld
}

var (
	finishedJobsTopicName        = os.Getenv("FINISHED_JOBS_TOPIC")
	incomingJobsSubscriptionName = os.Getenv("INCOMING_JOBS_SUBSCRIPTION")
)

func (app *App) initPubSub() {
	ctx := context.Background()
	psClient, err := pubsub.NewClient(ctx, "")
	if err != nil {
		app.logger.Fatal("pubsub client", zap.Error(err))
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

type ccurld struct {
	ccurlPath    string
	ccurlTimeout time.Duration

	minWeightMagnitude string
	pipe               *os.File
	cmd                *exec.Cmd
}

func newCcurld(cPath string, to time.Duration) (*ccurld, error) {
	c := &ccurld{
		ccurlPath:    cPath,
		ccurlTimeout: to,
	}

	if _, err := os.Stat(c.ccurlPath); err != nil && os.IsNotExist(err) {
		return nil, fmt.Errorf("expected ccurl executable but found nothing at %q", c.ccurlPath)
	}

	return c, nil
}

func (c *ccurld) Start(pipePath, mwm string) error {
	if mwm == "" {
		mwm = "13" // Testnet magnitude
	}

	if pipePath == "" {
		p, err := os.Getwd()
		if err != nil {
			return err
		}
		pipePath = filepath.Join(p, "ccurld-pipe")
	}

	// Ccurld creates the pipe itself at the moment and will complain if the pipe
	// exists already, so try to remove it.
	if err := os.Remove(pipePath); err != nil && !os.IsNotExist(err) {
		return err
	}

	cmd := exec.Command(c.ccurlPath, mwm, pipePath)
	if err := cmd.Start(); err != nil {
		return err
	}
	c.cmd = cmd

	// Wait for the pipe to appear or time out.
	fc := make(chan struct{})
	defer close(fc)
	go func() {
		for {
			if _, err := os.Stat(pipePath); err == nil {
				fc <- struct{}{}
				return
			}
		}
	}()

	select {
	case <-fc:
	case <-time.After(3 * time.Second):
		return fmt.Errorf("waited 3 seconds for pipe to appear")
	}

	wp, err := os.OpenFile(pipePath, os.O_RDWR, 0600)
	if err != nil {
		return err
	}

	c.pipe = wp

	return nil
}

func (c *ccurld) Write(s string) (int, error) {
	return c.pipe.Write([]byte(s))
}

func (c *ccurld) Read() (string, error) {
	outB := &bytes.Buffer{}
	_, err := io.Copy(outB, c.pipe)
	if err != nil {
		return "", err
	}

	return outB.String(), nil
}

func (c *ccurld) Wait() error {
	return c.cmd.Wait() // TODO
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

	// First setup the ccurld
	ccurlPath := os.Getenv("CCURL_PATH")
	pipePath := os.Getenv("PIPE_PATH")
	minWeightMagnitude := os.Getenv("MIN_WEIGHT_MAGNITUDE")

	ccurlTimeout := 120 * time.Second
	to, err := strconv.Atoi(os.Getenv("CCURL_TIMEOUT"))
	if err == nil {
		ccurlTimeout = time.Duration(to) * time.Second
	}

	ccd, err := newCcurld(ccurlPath, ccurlTimeout)
	if err != nil {
		app.logger.Fatal("initializing ccurld", zap.Error(err))
	}
	if err := ccd.Start(pipePath, minWeightMagnitude); err != nil {
		app.logger.Fatal("starting ccurld", zap.Error(err))
	}

	app.ccurld = ccd

	app.initPubSub()

	// TODO: add graceful shutdown
	app.logger.Info("starting worker")
	go func() {
		err = app.Worker()
		if err != nil {
			app.logger.Fatal("pubsub consumer", zap.Error(err))
		}
	}()

	app.logger.Info("starting ccurld")
	app.logger.Fatal("ccurl daemon", zap.Error(ccd.Wait()))
}
