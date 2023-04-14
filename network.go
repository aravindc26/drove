package main

import (
	"drove/util"
	"drove/zookeeper"
	"github.com/go-zookeeper/zk"
	"log"
)

func leaderConnectionDisruption(leaderCancel *util.MyChannel) zk.EventCallback {
	cbk := func(event zk.Event) {
		if event.Type != zk.EventSession || event.State != zk.StateDisconnected {
			return
		}
		log.Println("cancelling leadership")
		leaderCancel.SafeClose()
	}

	return cbk
}

// in case of disconnection we need to shutdown all our goroutines in our pool
func connectionDisruption(zkClient *zookeeper.Zk, wm *util.WorkerManager) zk.EventCallback {
	cbk := func(event zk.Event) {
		if event.Type != zk.EventSession || event.State != zk.StateDisconnected {
			return
		}
		zkClient.Disconnected = true
		log.Println("shutting down jobs")
		wm.ShutDownJobs()
	}

	return cbk
}

func reconnectedAfterDisruption(zkClient *zookeeper.Zk, currNode zookeeper.Node) zk.EventCallback {
	cbk := func(event zk.Event) {
		if event.Type != zk.EventSession || event.State != zk.StateHasSession {
			return
		}

		log.Printf("reconnected callback %v", zkClient.Disconnected)
		if !zkClient.Disconnected {
			return
		}

		go func() {
			err := zkClient.RegisterToLiveNodes(currNode)
			if err != nil {
				log.Println("error registering to live nodes", err)
				return
			}

			_, err = participateLeaderElection(zkClient, currNode)
			if err != nil {
				return
			}
		}()
	}
	return cbk
}
