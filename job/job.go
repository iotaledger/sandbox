package job

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	giota "github.com/iotaledger/iota.lib.go"
	uuid "github.com/satori/go.uuid"
)

type JobStatus string

const (
	JobStatusQueued   = "QUEUED"
	JobStatusRunning  = "RUNNING"
	JobStatusFailed   = "FAILED"
	JobStatusAborted  = "ABORTED"
	JobStatusFinished = "FINISHED"
)

type IRIJob struct {
	ID                    uuid.UUID                     `json:"id"`
	Status                JobStatus                     `json:"status"`
	CreatedAt             int64                         `json:"createdAt"`
	StartedAt             *int64                        `json:"startedAt"`
	FinishedAt            *int64                        `json:"finishedAt"`
	Command               string                        `json:"command"`
	AttachToTangleRequest *giota.AttachToTangleRequest  `json:"attachToTangleRequest,omitempty"`
	AttachToTangleRespose *giota.AttachToTangleResponse `json:"attachToTangleResponse,omitempty"`
	Error                 *JobError                     `json:"error,omitempty"`
}

func (ij *IRIJob) UnmarshalJSON(data []byte) error {
	var aux struct {
		ID                    uuid.UUID                     `json:"id"`
		Status                JobStatus                     `json:"status"`
		CreatedAt             int64                         `json:"createdAt"`
		StartedAt             *int64                        `json:"startedAt"`
		FinishedAt            *int64                        `json:"finishedAt"`
		Command               string                        `json:"command"`
		AttachToTangleRequest *giota.AttachToTangleRequest  `json:"attachToTangleRequest,omitempty"`
		AttachToTangleRespose *giota.AttachToTangleResponse `json:"attachToTangleResponse,omitempty"`
		Error                 *JobError                     `json:"error,omitempty"`
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&aux); err != nil {
		return err
	}

	switch aux.Command {
	case "attachToTangle":
		if aux.AttachToTangleRequest == nil {
			return fmt.Errorf("attachToTangleRequest object missing")
		}
	default:
		return fmt.Errorf("no command supplied")
	}

	ij.ID = aux.ID
	ij.Status = aux.Status
	ij.CreatedAt = aux.CreatedAt
	ij.StartedAt = aux.StartedAt
	ij.FinishedAt = aux.FinishedAt
	ij.Command = aux.Command
	ij.AttachToTangleRequest = aux.AttachToTangleRequest
	ij.AttachToTangleRespose = aux.AttachToTangleRespose
	ij.Error = aux.Error

	return nil
}

func NewIRIJob(cmd string) *IRIJob {
	return &IRIJob{
		ID:        uuid.NewV4(),
		Status:    JobStatusQueued,
		CreatedAt: time.Now().Unix(),
		Command:   cmd,
	}
}

type JobError struct {
	Message string `json:"message"`
}

func (j *JobError) String() string {
	return j.Message
}

func (j *JobError) Error() string {
	return j.Message
}
