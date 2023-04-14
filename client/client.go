package client

import (
	"drove/constants"
	"fmt"
	"github.com/go-zookeeper/zk"
	"log"
	"time"
)

type Client struct {
	zkHost string
	zkPort string
	conn   *zk.Conn
}

func CreateSchedulerClient(zkHost, zkPort string) (*Client, error) {
	cli := &Client{zkHost: zkHost, zkPort: zkPort}
	conn, _, err := zk.Connect([]string{fmt.Sprintf("%s:%s", zkHost, zkHost)}, time.Millisecond*1000)
	if err != nil {
		return nil, err
	}

	cli.conn = conn
	return cli, err
}

func (cli *Client) ScheduleJob(jobName string) error {
	znode := fmt.Sprintf("%s/%s", constants.Jobs, jobName)
	_, err := cli.conn.Create(znode, []byte(""), 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		log.Println(fmt.Sprintf("error scheduling job\n"), err)
		return err
	}
	return nil
}

func (cli *Client) DeleteJob(jobName string) error {
	znode := fmt.Sprintf("%s/%s", constants.DeleteJobs, jobName)
	_, err := cli.conn.Create(znode, []byte(""), 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		log.Println(fmt.Sprintf("error scheduling job\n"), err)
		return err
	}
	return nil
}
