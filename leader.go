package main

import (
	"bytes"
	"container/heap"
	"drove/constants"
	"drove/util"
	"drove/zookeeper"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-zookeeper/zk"
	"log"
	"net/http"
	"time"
)

// NodeHeap A max heap implementation of zookeeper.Node
type NodeHeap []zookeeper.Node

func (h NodeHeap) Len() int {
	return len(h)
}

func (h NodeHeap) Less(i, j int) bool {
	return h[i].RemainingCapacity > h[j].RemainingCapacity
}

func (h NodeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *NodeHeap) Push(x interface{}) {
	*h = append(*h, x.(zookeeper.Node))
}

func (h *NodeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func participateLeaderElection(zkClient *zookeeper.Zk, currNode zookeeper.Node) (bool, error) {
	err := zkClient.ParticipateLeaderElection(currNode)
	if err != nil {
		log.Println("error participating leader election", err)
		return false, err
	}

	leader, err := zkClient.GetLeader()
	if err != nil {
		log.Println("error fetching leader ", err)
		return false, err
	}

	log.Printf("leader %+v\n", leader)
	log.Printf("current node %+v", currNode)
	log.Printf("zksessionid %d", zkClient.SessionID())

	isLeader := leader.Host == currNode.Host && leader.Port == currNode.Port && leader.SessionID == zkClient.SessionID()
	leaderChan := util.NewMyChannel()
	if isLeader {
		log.Println("I am leader")
		zkClient.RegisterLeaderDisconnectionCallBack(leaderConnectionDisruption(leaderChan))
		go leaderWork(leaderChan, zkClient)
	} else {
		log.Println("I am just a worker")
	}

	electionChildWatcher, err := zkClient.SubscribeToChildrenChanges(constants.Election)
	if err != nil {
		log.Println(fmt.Sprintf("error registering child electionChildWatcher form %s node ", constants.Election), err)
		if leaderChan != nil {
			leaderChan.SafeClose()
		}
		return isLeader, err
	}
	zkClient.SetElectionWatcherAndCancel(electionChildWatcher, leaderChan)
	return isLeader, nil
}

func leaderWork(leaderChan *util.MyChannel, zkClient *zookeeper.Zk) {
	for {
		select {
		case <-leaderChan.C:
			log.Println("closing leader work")
			return
		case <-time.Tick(time.Second * 10):
			log.Println("looking for fresh jobs to assign")
			allocateJobsToWorkers(zkClient)
		}
	}
}

func allocateJobsToWorkers(zkClient *zookeeper.Zk) {
	unassigned, deleteNodes, err := zkClient.GetUnassignedAndDeleteJobs()
	if err != nil {
		log.Println("error fetching jobs ", err)
		return
	}

	nodes, err := zkClient.GetLiveNodes()
	if err != nil {
		log.Println("error fetching live nodes ", err)
		return
	}

	nodeHeap := &NodeHeap{}
	*nodeHeap = nodes

	heap.Init(nodeHeap)

	notDelegated := make(map[string]bool)
	for _, v := range unassigned {
		notDelegated[v] = true
	}

	for _, v := range unassigned {
	outer:
		if nodeHeap.Len() == 0 {
			log.Println("provision more nodes, no free nodes available")
			break
		} else {
			log.Printf("heap size %d\n", nodeHeap.Len())
		}

		pop := heap.Pop(nodeHeap)
		n := pop.(zookeeper.Node)
		if n.RemainingCapacity == 0 {
			log.Println("provision more nodes, no free nodes available")
			break
		}
		workerRespCode, err := assignWork(n, v)
		if err != nil {
			log.Printf("error assigning work %s to node %+v %+v\n", v, n, err)
			if workerRespCode > 0 {
				log.Printf("failed with response code %d", workerRespCode)
			}
			if workerRespCode == ErrorAcquiringJobLock {
				// this could mean job is already assigned, so we skip the current job and retain the worker in our heap
				log.Println("Error acquiring job lock, possibly assigned so skipping assignment")
				n.RemainingCapacity = n.RemainingCapacity - 1
				heap.Push(nodeHeap, n)
				continue
			} else if workerRespCode == ErrorJobDoesNotExist || workerRespCode == ErrorJobTaskDoesNotExist {
				// we skip if job does not exist
				n.RemainingCapacity = n.RemainingCapacity - 1
				heap.Push(nodeHeap, n)
				continue
			} else if workerRespCode == ErrorUpdatingLiveNode || workerRespCode == ErrorNodeDoesNotExist {
				// this means there could be some issue with live node losing connection with zookeeper,
				// so we don't retain the worker in the heap, and we retry the job with some other worker

				// since we are not retaining the worker in the heap this will not run infinitely since
				// workers are going to get exhausted
				log.Println("retrying job with some other node")
				goto outer
			} else {
				continue
			}
		}

		n.RemainingCapacity = n.RemainingCapacity - 1
		heap.Push(nodeHeap, n)

		delete(notDelegated, v)
		log.Printf("finished assigning %s\n", v)
	}

	var nd []string
	for k := range notDelegated {
		nd = append(nd, k)
	}

	log.Printf("not delegated %+v", nd)

	notDeleted := make(map[string]bool)
	for _, v := range deleteNodes {
		notDeleted[v] = true
	}
	for _, v := range deleteNodes {
		node, err := zkClient.GetAssignedJobNode(v)
		if err != nil {
			if err != zk.ErrInvalidPath {
				log.Printf("node %s could have been already deleted", v)
			} else {
				log.Printf("error getting assigned live node %s  %+v", v, err)
				continue
			}
		}

		_, err = unassignWork(node, v)
		if err != nil {
			log.Printf("error unassigning live node %s  %+v", v, err)
			continue
		}

		err = zkClient.DeleteAssignedJobNode(v)
		if err != nil {
			log.Printf("error deleting live node %s  %+v", v, err)
			continue
		}

		delete(notDeleted, v)
	}

	var nDel []string
	for k := range notDeleted {
		nd = append(nd, k)
	}

	log.Printf("not deleted %+v", nDel)
}

func postRequest(node zookeeper.Node, path string, body interface{}) (WorkerResponse, error) {
	url := fmt.Sprintf("http://localhost:%s%s", node.Port, path)
	var jsonData, err = json.Marshal(&body)
	if err != nil {
		return WorkerResponse{}, err
	}

	request, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return WorkerResponse{}, err
	}
	request.Header.Set("Content-Type", "application/json; charset=UTF-8")

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return WorkerResponse{}, err
	}
	defer response.Body.Close()

	log.Println("response Status:", response.Status)
	log.Println("response Headers:", response.Header)
	var workerResp WorkerResponse
	err = json.NewDecoder(response.Body).Decode(&workerResp)
	log.Printf("response Body: %+v", workerResp)

	return workerResp, err
}

func unassignWork(node zookeeper.Node, job string) (int, error) {
	workerJob := WorkerJobs{Job: job}
	workerResp, err := postRequest(node, "/jobs/unassign", workerJob)
	if err != nil {
		return 0, err
	}
	if workerResp.Code > 0 {
		return workerResp.Code, errors.New("error unassigning work to node")
	}
	return workerResp.Code, nil
}

func assignWork(node zookeeper.Node, job string) (int, error) {
	workerJob := WorkerJobs{Job: job}
	workerResp, err := postRequest(node, "/jobs/assign", workerJob)
	if err != nil {
		return 0, err
	}
	if workerResp.Code > 0 {
		return workerResp.Code, errors.New("error assigning work to node")
	}
	return workerResp.Code, nil
}
