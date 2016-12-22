package job

import (
	"encoding/json"
	"errors"

	"github.com/oleiade/lane"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type JobQueue interface {
	EnqueueJob(*IRIJob) error
	DequeueJob() (*IRIJob, error)
}

type MemoryQueue struct {
	q *lane.Queue
}

func NewMemoryQueue() *MemoryQueue {
	q := lane.NewQueue()
	return &MemoryQueue{q: q}
}

func (mq *MemoryQueue) DequeueJob() (*IRIJob, error) {
	it := mq.q.Dequeue()
	if it == nil {
		return nil, nil
	}

	return it.(*IRIJob), nil
}

func (mq *MemoryQueue) EnqueueJob(it *IRIJob) error {
	mq.q.Enqueue(it)
	return nil
}

type SQSQueue struct {
	c        *sqs.SQS
	Name     string
	QueueUrl string
}

func NewSQSQueue(sess *session.Session, name string) (*SQSQueue, error) {
	ss := sqs.New(sess)
	sq := &SQSQueue{c: ss}

	qurl, err := sq.createJobQeue(name)
	if err != nil {
		return nil, err
	}

	sq.QueueUrl = qurl

	return sq, nil
}

// createJobQueue calls CreateQueue with the given name, which then
// either returns a new queue url or an existing one.
func (sq *SQSQueue) createJobQeue(name string) (qurl string, err error) {
	params := &sqs.CreateQueueInput{
		QueueName: &name,
		Attributes: map[string]*string{
			"VisibilityTimeout": aws.String("3600"), // give each job one hour to complete
		},
	}

	resp, err := sq.c.CreateQueue(params)

	if err != nil {
		return
	} else if resp.QueueUrl == nil {
		return qurl, errors.New("did not get a QueueUrl from aws")
	}

	qurl = *resp.QueueUrl

	return
}

func (sq *SQSQueue) DequeueJob() (*IRIJob, error) {
	out := new(sqs.ReceiveMessageOutput)

	in := &sqs.ReceiveMessageInput{
		QueueUrl:            &sq.QueueUrl,
		MaxNumberOfMessages: aws.Int64(1),
		WaitTimeSeconds:     aws.Int64(10),
	}
	out, err := sq.c.ReceiveMessage(in)

	if len(out.Messages) == 0 || out.Messages[0] == nil || out.Messages[0].Body == nil {
		return nil, nil
	}

	j := &IRIJob{}
	handle := out.Messages[0].ReceiptHandle
	err = json.Unmarshal([]byte(*out.Messages[0].Body), j)
	if err != nil {
		// If we cannot decode the body then just drop the message.
		_ = sq.deleteJob(handle)
		return nil, err
	}

	// Message was received successfully, so delete it from the queue
	// and if we cannot delete it then there's not much we can do either.
	_ = sq.deleteJob(handle)

	return j, err
}

func (sq *SQSQueue) EnqueueJob(it *IRIJob) error {
	b, err := json.Marshal(it)
	if err != nil {
		return err
	}

	in := &sqs.SendMessageInput{
		MessageBody: aws.String(string(b)),
		QueueUrl:    &sq.QueueUrl,
	}
	_, err = sq.c.SendMessage(in)

	return err
}

func (sq *SQSQueue) deleteJob(handle *string) error {
	params := &sqs.DeleteMessageInput{
		QueueUrl:      &sq.QueueUrl,
		ReceiptHandle: handle,
	}
	_, err := sq.c.DeleteMessage(params)

	if err != nil {
		return err
	}

	return nil
}
