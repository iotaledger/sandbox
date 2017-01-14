package main

/*
#cgo LDFLAGS: -L. -lccurl
#include <ccurl/ccurl.h>
#include <stdlib.h>
*/
import "C"
import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"
	"unsafe"

	"github.com/iotaledger/sandbox/job"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	giota "github.com/iotaledger/iota.lib.go"
	"github.com/uber-go/zap"
)

func (app *App) failJob(j *job.IRIJob, errMsg string) {
	j.Error = &job.JobError{Message: errMsg}
	j.Status = job.JobStatusFailed
	fin := time.Now().Unix()
	j.FinishedAt = &fin

	err := app.finishedJobs.EnqueueJob(j)
	if err != nil {
		app.logger.Error("finished job", zap.String("method", "EnqueueJob"), zap.Error(err))
	}
	app.logger.Debug("failJob enqueued", zap.String("errMsg", errMsg), zap.Object("job", j))
}

func (app *App) HandleAttachToTangle(ctx context.Context, j *job.IRIJob) {
	if j.AttachToTangleRequest == nil {
		app.failJob(j, "no request attachToTangleRequest supplied")
		return
	}

	if len(j.AttachToTangleRequest.Trytes) < 1 {
		app.failJob(j, "no trytes supplied")
		return
	}

	outTrytes := []string{}
	var prevTxHash *giota.Hash = nil

	for _, ts := range j.AttachToTangleRequest.Trytes {
		if !giota.ValidTransactionTrytes(ts) {
			app.failJob(j, "invalid trytes")
			return
		}

		trits := giota.TrytesToTrits(ts)
		if prevTxHash == nil {
			copy(trits[giota.TrunkTransactionTrinaryOffset:giota.TrunkTransactionTrinaryOffset+giota.TrunkTransactionTrinarySize], giota.TrytesToTrits(j.AttachToTangleRequest.TrunkTransaction))
			copy(trits[giota.BranchTransactionTrinaryOffset:giota.BranchTransactionTrinaryOffset+giota.BranchTransactionTrinarySize], giota.TrytesToTrits(j.AttachToTangleRequest.BranchTransaction))
		} else {
			copy(trits[giota.TrunkTransactionTrinaryOffset:giota.TrunkTransactionTrinaryOffset+giota.TrunkTransactionTrinarySize], prevTxHash.Trits())
			copy(trits[giota.BranchTransactionTrinaryOffset:giota.BranchTransactionTrinaryOffset+giota.BranchTransactionTrinarySize], giota.TrytesToTrits(j.AttachToTangleRequest.TrunkTransaction))
		}

		cTrytes := C.CString(giota.TritsToTrytes(trits))

		out := C.ccurl_pow(cTrytes, C.int(j.AttachToTangleRequest.MinWeightMagnitude))
		C.free(unsafe.Pointer(cTrytes))
		if out == nil {
			app.failJob(j, "pow failed")
			return
		}

		s := C.GoString(out)
		prevTxHash = giota.HashFromTrits(giota.TrytesToTrits(s))
		outTrytes = append(outTrytes, s)
		C.free(unsafe.Pointer(out))
	}

	j.AttachToTangleRespose = &giota.AttachToTangleResponse{Trytes: outTrytes}
	j.Status = job.JobStatusFinished
	finishedAt := time.Now().Unix()
	j.FinishedAt = &finishedAt
	err := app.finishedJobs.EnqueueJob(j)
	if err != nil {
		app.logger.Error("finished job", zap.String("method", "EnqueueJob"), zap.Error(err))
	}
	app.logger.Debug("new finished job enqueued")
}

func (app *App) Worker() error {
	for {
		j, err := app.incomingJobs.DequeueJob()
		if err != nil || j == nil {
			app.logger.Debug("got no new incoming jobs")
			continue
		}

		startedAt := time.Now().Unix()
		j.StartedAt = &startedAt
		app.logger.Debug("new job", zap.Object("job", j))
		switch j.Command {
		default:
			app.failJob(j, fmt.Sprintf("unknown command %q", j.Command))
			continue
		case "attachToTangle":
			ctx, _ := context.WithTimeout(context.Background(), 1*time.Minute)
			app.HandleAttachToTangle(ctx, j)
		}
	}
}

type App struct {
	//ccurlPath    string
	ccurlLoopCount C.size_t
	ccurlTimeout   time.Duration

	logger       zap.Logger
	incomingJobs job.JobQueue
	finishedJobs job.JobQueue
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

	if os.Getenv("DEBUG") == "1" {
		app.logger = zap.New(zap.NewJSONEncoder(), zap.DebugLevel)
	} else {
		app.logger = zap.New(zap.NewJSONEncoder())
	}

	C.ccurl_pow_init()

	app.ccurlLoopCount = 32
	lc, err := strconv.Atoi(os.Getenv("CCURL_LOOP_COUNT"))
	if err != nil {
		app.ccurlLoopCount = C.size_t(lc)
	}
	C.ccurl_pow_set_loop_count(app.ccurlLoopCount)

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

	app.ccurlTimeout = 120 * time.Second
	to, err := strconv.Atoi(os.Getenv("CCURL_TIMEOUT"))
	if err != nil {
		app.ccurlTimeout = time.Duration(to) * time.Second
	}

	// XXX: add graceful shutdown
	app.logger.Info("starting worker")
	app.Worker()
	C.ccurl_pow_finalize()
}
