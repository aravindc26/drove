package zookeeper

import (
	"drove/constants"
	"drove/util"
	"encoding/json"
	"fmt"
	"github.com/go-zookeeper/zk"
	"log"
	"sync"
	"time"
)

type Node struct {
	Host              string
	Port              string
	MaxCapacity       int
	RemainingCapacity int
	SessionID         int64
}

type Zk struct {
	host                         string
	port                         string
	conn                         *zk.Conn
	leaderDisconnectionCallBack  zk.EventCallback
	connectionDisruptionCallBack zk.EventCallback
	connectReestabCallBack       zk.EventCallback
	mu                           sync.Mutex
	Disconnected                 bool
	ElectionChildWatcher         <-chan zk.Event
	LeaderCancel                 *util.MyChannel
	lmu                          sync.Mutex
}

func Create(host string, port string) (*Zk, error) {
	client := &Zk{host: host, port: port, Disconnected: false}

	var eventCallBack zk.EventCallback = client.callBacks

	conn, _, err := zk.Connect([]string{fmt.Sprintf("%s:%s", host, port)}, time.Millisecond*1000, zk.WithEventCallback(eventCallBack))
	if err != nil {
		return nil, err
	}

	client.conn = conn

	return client, nil
}

func (client *Zk) SetElectionWatcherAndCancel(electionChildWatcher <-chan zk.Event, leaderCancel *util.MyChannel) {
	client.lmu.Lock()
	defer client.lmu.Unlock()

	client.ElectionChildWatcher = electionChildWatcher
	client.LeaderCancel = leaderCancel
}

func (client *Zk) callBacks(event zk.Event) {
	log.Printf("event occured %+v", event)
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.leaderDisconnectionCallBack != nil {
		client.leaderDisconnectionCallBack(event)
	}

	if client.connectionDisruptionCallBack != nil {
		client.connectionDisruptionCallBack(event)
	}

	if client.connectReestabCallBack != nil {
		client.connectReestabCallBack(event)
	}
}

func (client *Zk) InitializeZooKeeper() error {
	exists, _, err := client.conn.Exists(constants.Election)
	if err != nil {
		return err
	}

	if !exists {
		_, err := client.conn.Create(constants.Election, []byte("election takes place here"), 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			log.Println(fmt.Sprintf("error creating node: %s", constants.Election), err)
			return err
		}
	}

	exists, _, err = client.conn.Exists(constants.LiveNodes)
	if err != nil {
		return err
	}

	if !exists {
		_, err = client.conn.Create(constants.LiveNodes, []byte("register live nodes here"), 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			log.Println(fmt.Sprintf("error creating node: %s", constants.LiveNodes), err)
			return err
		}
	}

	_, err = client.conn.Create(constants.Jobs, []byte("register live nodes here"), 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		log.Println(fmt.Sprintf("error creating node: %s", constants.Jobs), err)
		return err
	}

	_, err = client.conn.Create(constants.DeleteJobs, []byte("register live nodes here"), 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		log.Println(fmt.Sprintf("error creating node: %s", constants.Jobs), err)
		return err
	}

	return nil
}

func (client *Zk) ParticipateLeaderElection(node Node) error {
	exists, _, err := client.conn.Exists(constants.Leader)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	marshal, err := json.Marshal(node)
	if err != nil {
		return err
	}

	_, err = client.conn.Create(constants.Leader, marshal, 1, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		return err
	}
	return nil
}

func (client *Zk) GetLeader() (Node, error) {
	resp, _, err := client.conn.Get(constants.Leader)
	if err != nil {
		return Node{}, err
	}

	var node Node
	err = json.Unmarshal(resp, &node)
	if err != nil {
		return Node{}, err
	}

	return node, nil
}

func (client *Zk) SubscribeToChildrenChanges(path string) (<-chan zk.Event, error) {
	_, _, events, err := client.conn.ChildrenW(path)
	return events, err
}

func (client *Zk) RegisterToLiveNodes(node Node) error {
	identifier := fmt.Sprintf("%s_%s", node.Host, node.Port)
	marshal, err := json.Marshal(node)
	if err != nil {
		return err
	}
	zNode := fmt.Sprintf("%s/%s", constants.LiveNodes, identifier)

	err = client.conn.Delete(zNode, -1)
	if err != nil && err != zk.ErrNoNode {
		return err
	}

	_, err = client.conn.Create(zNode, marshal, 1, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
	return nil
}

func (client *Zk) UpdateLiveNode(node Node) error {
	identifier := fmt.Sprintf("%s_%s", node.Host, node.Port)
	marshal, err := json.Marshal(node)
	if err != nil {
		return err
	}

	zNode := fmt.Sprintf("%s/%s", constants.LiveNodes, identifier)
	_, err = client.conn.Set(zNode, marshal, -1)
	return err
}

func (client *Zk) RegisterLeaderDisconnectionCallBack(eventCallBack zk.EventCallback) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.leaderDisconnectionCallBack = eventCallBack
}

func (client *Zk) RegisterConnectionDisruptionCallBack(eventCallBack zk.EventCallback) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.connectionDisruptionCallBack = eventCallBack
}

func (client *Zk) RegisterConnectionReestablishmentCallBack(callback zk.EventCallback) {
	client.mu.Lock()
	defer client.mu.Unlock()

	client.connectReestabCallBack = callback
}

func (client *Zk) SessionID() int64 {
	client.conn.SessionID()
	return client.conn.SessionID()
}

func (client *Zk) GetUnassignedAndDeleteJobs() ([]string, []string, error) {
	children, _, err := client.conn.Children(constants.Jobs)
	if err != nil {
		return nil, nil, err
	}

	var unassignedJobs []string

	for _, v := range children {
		_, stat, err := client.conn.Get(fmt.Sprintf("%s/%s", constants.Jobs, v))
		if err != nil {
			log.Println(fmt.Sprintf("error fetching node %s", v), err)
			return nil, nil, err
		}

		//log.Printf("node %s num children %d", v, stat.NumChildren)

		if stat.NumChildren == 0 {
			unassignedJobs = append(unassignedJobs, v)
		}
	}

	var deleteJobs []string
	deleteJobsChildren, _, err := client.conn.Children(constants.DeleteJobs)
	if err != nil {
		return nil, nil, err
	}

	for _, v := range deleteJobsChildren {
		deleteJobs = append(deleteJobs, v)
	}

	return unassignedJobs, deleteJobs, nil
}

func (client *Zk) FlushDeleteJobs(deleteJobs []string) error {
	for _, v := range deleteJobs {
		err := client.conn.Delete(fmt.Sprintf("%s/%s", constants.Jobs, v), -1)
		if err != nil {
			return err
		}

		err = client.conn.Delete(fmt.Sprintf("%s/%s", constants.DeleteJobs, v), -1)
		if err != nil {
			return err
		}
	}

	return nil
}

func (client *Zk) MarkJobAssigned(jobName string, n Node) error {
	marshal, err := json.Marshal(n)
	if err != nil {
		return err
	}

	path := fmt.Sprintf("%s/%s", constants.Jobs, jobName)
	exists, _, err := client.conn.Exists(path)
	if err != nil {
		return err
	}

	if !exists {
		return zk.ErrInvalidPath
	}

	path = fmt.Sprintf("%s/%s", path, constants.Assigned)
	_, err = client.conn.Create(path, marshal, 1, zk.WorldACL(zk.PermAll))
	return err
}

func (client *Zk) GetLiveNodes() ([]Node, error) {
	children, _, err := client.conn.Children(constants.LiveNodes)
	if err != nil {
		return nil, err
	}

	var result []Node
	for _, v := range children {
		child, _, err := client.conn.Get(fmt.Sprintf("%s/%s", constants.LiveNodes, v))
		if err != nil {
			log.Printf("err fetching %s", fmt.Sprintf("%s/%s", constants.LiveNodes, v))
			return nil, err
		}

		var n Node

		err = json.Unmarshal(child, &n)
		if err != nil {
			return nil, err
		}

		result = append(result, n)
	}

	return result, nil
}

func (client *Zk) GetAssignedJobNode(path string) (Node, error) {
	node, _, err := client.conn.Get(fmt.Sprintf("%s/%s/%s", constants.Jobs, path, constants.Assigned))
	if err != nil {
		log.Printf("err fetching %s", fmt.Sprintf("%s/%s/%s", constants.Jobs, path, constants.Assigned))
		return Node{}, err
	}

	var n Node

	err = json.Unmarshal(node, &n)
	return n, err
}

func (client *Zk) DeleteAssignmentJobNode(path string) error {
	err := client.conn.Delete(fmt.Sprintf("%s/%s/%s", constants.Jobs, path, constants.Assigned), -1)
	if err != nil && err != zk.ErrInvalidPath {
		return err
	}
	return nil
}

func (client *Zk) DeleteAssignedJobNode(path string) error {
	err := client.conn.Delete(fmt.Sprintf("%s/%s/%s", constants.Jobs, path, constants.Assigned), -1)
	if err != nil && err != zk.ErrInvalidPath && err != zk.ErrNoNode {
		return err
	}

	err = client.conn.Delete(fmt.Sprintf("%s/%s", constants.Jobs, path), -1)
	if err != nil && err != zk.ErrInvalidPath && err != zk.ErrNoNode {
		return err
	}

	err = client.conn.Delete(fmt.Sprintf("%s/%s", constants.DeleteJobs, path), -1)
	if err != nil && err != zk.ErrInvalidPath && err != zk.ErrNoNode {
		return err
	}
	return nil
}
