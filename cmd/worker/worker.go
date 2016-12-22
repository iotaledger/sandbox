package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"time"

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
			if j.AttachToTangleRequest == nil {
				app.failJob(j, "no request attachToTangleRequest supplied")
				continue
			}

			if len(j.AttachToTangleRequest.Trytes) < 1 {
				app.failJob(j, "no trytes supplied")
				continue
			}

			cmd := exec.Command(app.ccurlPath, strconv.FormatInt(j.AttachToTangleRequest.MinWeightMagnitude, 10), j.AttachToTangleRequest.Trytes[0])
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				app.failJob(j, err.Error())
				continue
			}

			if err := cmd.Start(); err != nil {
				app.failJob(j, err.Error())
				continue
			}

			if err := cmd.Wait(); err != nil {
				app.failJob(j, err.Error())
				continue
			}

			trytes, err := ioutil.ReadAll(stdout)
			if err != nil {
				app.failJob(j, err.Error())
				continue
			}

			j.AttachToTangleRespose = &giota.AttachToTangleResponse{Trytes: []string{string(trytes)}}
			j.Status = job.JobStatusFinished
			finishedAt := time.Now().Unix()
			j.FinishedAt = &finishedAt
			err = app.finishedJobs.EnqueueJob(j)
			if err != nil {
				app.logger.Error("finished job", zap.String("method", "EnqueueJob"), zap.Error(err))
			}
			app.logger.Debug("new finished job enqueued")
		}
	}
}

type App struct {
	ccurlPath string

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

	app.ccurlPath = os.Getenv("CCURL_PATH")
	if app.ccurlPath == "" {
		app.logger.Fatal("$CCURL_PATH not set")
	}

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

	// XXX: add graceful shutdown
	app.logger.Info("starting worker")
	app.Worker()
}
