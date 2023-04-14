package main

import (
	"drove/util"
	"drove/zookeeper"
	"fmt"
	"github.com/go-zookeeper/zk"
	"log"
	"net/http"
	"os"
	"os/signal"
)

func main() {
	args := os.Args[1:]
	interrupt := make(chan os.Signal)
	signal.Notify(interrupt, os.Interrupt)

	zooKeeperHost := args[0]
	zooKeeperPort := args[1]

	currentHost := args[2]
	currentPort := args[3]

	zkClient, err := zookeeper.Create(zooKeeperHost, zooKeeperPort)
	if err != nil {
		log.Println("error creating client ", err)
		return
	}

	repository := util.InitializeJobRepository()
	workerManager := util.CreateWorkerManager(5, repository)
	zkClient.RegisterConnectionDisruptionCallBack(connectionDisruption(zkClient, workerManager))

	err = zkClient.InitializeZooKeeper()
	if err != nil {
		log.Println("error initializing zookeeper", err)
		return
	}

	currNode := zookeeper.Node{Host: currentHost, Port: currentPort, MaxCapacity: 5, RemainingCapacity: 5, SessionID: zkClient.SessionID()}
	zkClient.RegisterConnectionReestablishmentCallBack(reconnectedAfterDisruption(zkClient, currNode))
	err = zkClient.RegisterToLiveNodes(currNode)
	if err != nil {
		log.Printf("error registering node %v\n", err)
		return
	}
	log.Println("registered node")

	_, err = participateLeaderElection(zkClient, currNode)
	if err != nil {
		return
	}

	srv := &http.Server{Addr: fmt.Sprintf(":%s", currNode.Port)}
	go StartWorkerServer(srv, workerManager, zkClient, currNode)
	for {
		select {
		case <-interrupt:
			log.Println("bye!")
			return
		case event := <-zkClient.ElectionChildWatcher:
			if event.Type == zk.EventNodeChildrenChanged {
				//if existing current node is acting as leader need to close while re-participating
				log.Println("participating in re-election")
				zkClient.LeaderCancel.SafeClose()
				_, err := participateLeaderElection(zkClient, currNode)
				if err != nil {
					log.Println("error participating leader election")
				}
			}
		}
	}
}
