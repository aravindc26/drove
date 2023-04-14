package main

import (
	"drove/util"
	"drove/zookeeper"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-zookeeper/zk"
	"log"
	"net/http"
	"os"
)

type WorkerJobs struct {
	Job string
}

// return error messages with proper error codes

type WorkerResponse struct {
	Code    int
	Message string
}

const (
	Success                     = 0
	InvalidJsonInput            = 1
	ExceededWorkerLimitExceeded = 2
	ErrorAcquiringJobLock       = 3
	ErrorJobDoesNotExist        = 4
	ErrorNodeDoesNotExist       = 5
	ErrorUpdatingLiveNode       = 6
	ErrorJobTaskDoesNotExist    = 7
	UnexpectedError             = 8
)

func assignJobs(workerManager *util.WorkerManager, client *zookeeper.Zk, node zookeeper.Node) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		var workerJobs WorkerJobs

		err := json.NewDecoder(request.Body).Decode(&workerJobs)
		if err != nil {
			writeErrorResponse(writer, WorkerResponse{Code: InvalidJsonInput, Message: err.Error()})
			return
		}

		err = client.MarkJobAssigned(workerJobs.Job, node)
		if err != nil {
			if err == zk.ErrInvalidPath {
				writeErrorResponse(writer, WorkerResponse{Code: ErrorJobDoesNotExist, Message: "Job does not exist"})
				return
			}
			writeErrorResponse(writer, WorkerResponse{Code: ErrorAcquiringJobLock, Message: err.Error()})
			return
		}

		completionCbk := func() {
			log.Printf("calling completion callback\n")
			err := client.DeleteAssignedJobNode(workerJobs.Job)
			if err != nil && err != zk.ErrNoNode {
				log.Printf("error deleting node %s %+v\n", workerJobs.Job, err)
				return
			}
		}

		err = workerManager.DelegateJob(workerJobs.Job, completionCbk)
		if err != nil {
			err := client.DeleteAssignmentJobNode(workerJobs.Job)
			if err != nil {
				writeErrorResponse(writer, WorkerResponse{Code: UnexpectedError, Message: err.Error()})
				return
			}
			if err == util.ErrWorkerLimitExceeded {
				writeErrorResponse(writer, WorkerResponse{Code: ExceededWorkerLimitExceeded, Message: err.Error()})
			} else if err == util.ErrJobDoesNotExist {
				writeErrorResponse(writer, WorkerResponse{Code: ErrorJobTaskDoesNotExist, Message: fmt.Sprintf("%s does not exist", workerJobs.Job)})
			}
			return
		}

		consumed, max := workerManager.GetCapacity()
		node.MaxCapacity = max
		node.RemainingCapacity = max - consumed

		err = client.UpdateLiveNode(node)
		if err != nil {
			if err == zk.ErrInvalidPath {
				writeErrorResponse(writer, WorkerResponse{Code: ErrorNodeDoesNotExist, Message: "The node is not registered in live node"})
				return
			}
			writeErrorResponse(writer, WorkerResponse{Code: ErrorUpdatingLiveNode, Message: "Error while updating live node"})
		}

		writeSuccessResponse(writer, WorkerResponse{Code: Success, Message: "Assigned Job"})
	}
}

func writeErrorResponse(response http.ResponseWriter, message WorkerResponse) {
	response.WriteHeader(http.StatusBadRequest)
	response.Header().Set("Content-Type", "application/json")
	jsonResp, _ := json.Marshal(message)
	_, _ = response.Write(jsonResp)
	return
}

func writeSuccessResponse(response http.ResponseWriter, message WorkerResponse) {
	response.WriteHeader(http.StatusOK)
	response.Header().Set("Content-Type", "application/json")
	jsonResp, _ := json.Marshal(message)
	_, _ = response.Write(jsonResp)
	return
}

func unassignJobs(workerManager *util.WorkerManager, client *zookeeper.Zk, node zookeeper.Node) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		var workerJobs WorkerJobs

		err := json.NewDecoder(request.Body).Decode(&workerJobs)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		workerManager.StopJob(workerJobs.Job)
		writeSuccessResponse(writer, WorkerResponse{Code: Success, Message: "UnAssigned Job"})
	}
}

func StartWorkerServer(srv *http.Server, workerManager *util.WorkerManager, client *zookeeper.Zk, node zookeeper.Node) {
	http.HandleFunc("/jobs/assign", assignJobs(workerManager, client, node))
	http.HandleFunc("/jobs/unassign", unassignJobs(workerManager, client, node))

	err := srv.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		log.Printf("server closed\n")
	} else if err != nil {
		log.Printf("error starting server: %s\n", err)
		os.Exit(1)
	}
}
