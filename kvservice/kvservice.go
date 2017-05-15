package kvservice

import (
	"errors"
	"fmt"
	"net/rpc"
	"sort"
	"time"
)

// Represents a key in the system.
type Key string

// Represent a value in the system.
type Value string

// An interface representing a connection to the key-value store. To
// create a new connection use the NewConnection() method.
type connection interface {
	// The 'constructor' for a new logical transaction object. This is the
	// only way to create a new transaction. The returned transaction must
	// correspond to a specific, reachable, node in the k-v service. If
	// none of the nodes are reachable then tx must be nil and error must
	// be set (non-nil).
	NewTX() (newTX tx, err error)

	// Close the connection.
	Close()
}

// An interface representing a client's transaction. To create a new
// transaction use the connection.NewTX() method.
type tx interface {
	// Retrieves a value v associated with a key k as part of this
	// transaction. If success is true then v contains the value
	// associated with k and err is nil. If success is false then the
	// tx has aborted, v is nil, and err is non-nil. If success is
	// false, then all future calls on this transaction must
	// immediately return success = false (indicating an earlier
	// abort).
	Get(k Key) (success bool, v Value, err error)

	// Associates a value v with a key k as part of this
	// transaction. If success is true then put was recoded
	// successfully, otherwise the transaction has aborted (see
	// above).
	Put(k Key, v Value) (success bool, err error)

	// Commits this transaction. If success is true then commit
	// succeeded, otherwise the transaction has aborted (see above).
	// txID represents the transactions's global sequence number
	// (which determines this transaction's position in the serialized
	// sequence of all the other transactions executed by the
	// service).
	Commit() (success bool, txID int, err error)

	// Aborts this transaction. This call always succeeds.
	Abort()
}

// // channel to tell heartbeat to stop
// var cancelHeartbeat chan bool

// The 'constructor' for a new logical connection object. This is the
// only way to create a new connection. Takes a set of k-v service
// node ip:port strings.
func NewConnection(nodes []string) connection {
	serverNodes := make([]string, len(nodes))
	copy(serverNodes, nodes)
	sort.Strings(serverNodes)
	return &KVConn{serverNodes: serverNodes}
}

type KVConn struct {
	client      *rpc.Client
	serverNodes []string
}

func (c *KVConn) newConnectionToSwarm() {
	var deadNodeIdxs []int
	defer c.deleteDeadNodes(deadNodeIdxs)

	sort.Strings(c.serverNodes)
	for idx, node := range c.serverNodes {
		client, err := rpc.Dial("tcp", node)
		if err != nil {
			deadNodeIdxs = append(deadNodeIdxs, idx)
			continue
		}
		c.client = client
		fmt.Println("Connected to server", node)
		return
	}

	// unable to connect to any node
	c.client = nil
}

func (c *KVConn) NewTX() (newTX tx, err error) {
	newTX = nil
	err = errors.New("Unable to connect to any nodes")

	for c.newConnectionToSwarm(); c.client != nil; {

		var txID int
		err = c.client.Call("ClientServer.NewTX", 0, &txID)
		if err != nil {
			fmt.Println("Error calling ClientServer.NewTX", err)
			continue
		}

		cancelChan := make(chan bool, 100)
		newTransaction := KVTransaction{txID: txID, finished: false, cancelChan: cancelChan, connection: c}
		go heartbeatServer(newTransaction)
		newTX = &newTransaction
		err = nil
		return
	}

	return
}

// Close the connection.
func (c *KVConn) Close() {
	if c.client != nil {
		c.client.Close()
		c.client = nil
	}
}

func heartbeatServer(tx KVTransaction) {
	timeoutDuration := 2000 * time.Millisecond
	heartbeatDuration := 200 * time.Millisecond

	args := tx.txID
	var reply bool

	for {
		if tx.connection.client == nil {
			time.Sleep(timeoutDuration)
			continue
		}
		call := tx.connection.client.Go("ClientServer.Heartbeat", &args, &reply, nil)
		select {
		case <-call.Done:
			time.Sleep(heartbeatDuration)
			continue
		case <-tx.cancelChan:
			tx.closeTX()
			return
		case <-time.After(timeoutDuration):
			tx.closeTX()
			return
		}
	}
}

type KVTransaction struct {
	txID       int
	finished   bool
	cancelChan chan bool
	connection *KVConn
}

type GetArgs struct {
	TxID int
	Key  string
}

type GetReply struct {
	Success bool
	Value   string
}

func (tx *KVTransaction) isConnectionAlive() bool {
	if tx.connection.client == nil {
		return false
	}
	timeoutDuration := 2000 * time.Millisecond

	args := tx.txID
	var reply bool
	call := tx.connection.client.Go("ClientServer.Heartbeat", &args, &reply, nil)
	select {
	case <-call.Done:
		return true
	case <-tx.cancelChan:
		return false
	case <-time.After(timeoutDuration):
		return false
	}
}

func (tx *KVTransaction) Get(k Key) (success bool, v Value, err error) {
	if tx.finished {
		return false, "", errors.New("Trasaction already closed")
	}

	args := GetArgs{TxID: tx.txID, Key: string(k)}
	var reply GetReply

	err = tx.connection.client.Call("ClientServer.Get", &args, &reply)
	if err == nil {
		success = reply.Success
		v = Value(reply.Value)
		return
	}

	success = false
	v = Value("")
	tx.cancelChan <- true
	return
}

type PutArgs struct {
	TxID  int
	Key   string
	Value string
}

type PutReply struct {
	Success bool
}

func (tx *KVTransaction) Put(k Key, v Value) (success bool, err error) {
	if tx.finished {
		return false, errors.New("Trasaction already closed")
	}

	args := PutArgs{TxID: tx.txID, Key: string(k), Value: string(v)}
	var reply PutReply

	err = tx.connection.client.Call("ClientServer.Put", &args, &reply)
	if err == nil {
		success = reply.Success
		return
	}

	success = false
	tx.cancelChan <- true
	return
}

type CommitOrAbortArgs struct {
	TxID int
}

func (tx *KVTransaction) Commit() (success bool, txID int, err error) {
	if tx.finished {
		return false, 0, errors.New("Trasaction already closed")
	}

	args := CommitOrAbortArgs{TxID: tx.txID}
	success = true

	defer func() {
		tx.cancelChan <- true //TODO what should we return on repeated commit call?
	}()

	err = tx.connection.client.Call("ClientServer.Commit", &args, &txID)
	confirmed := false

	// fmt.Println("Waiting 5 seconds so you can kill master to test")
	// time.Sleep(5*time.Second)
	// fmt.Println("Attempting to confirm commit now")

	// loop until server responds or all nodes are dead
	for !confirmed {
		if !tx.isConnectionAlive() || err != nil {
			// Sleep for a second to allow nodes to re-elect and reach consensus
			time.Sleep(1 * time.Second)
			tx.connection.newConnectionToSwarm()

			// All the nodes are dead
			if tx.connection.client == nil {
				break
			}
		}

		err = tx.connection.client.Call("ClientServer.ConfirmCommit", &args, &txID)
		fmt.Println("ConfirmCommit Response", err, txID)

		if err == nil && txID == -1 {
			// ConfirmCommit returned -1, transaction was not confirmed
			err = errors.New("No commit for that transaction ID!")
			break
		} else if err == nil {
			return
		}
	}

	success = false
	txID = 0
	return
}

func (tx *KVTransaction) Abort() {
	if tx.finished {
		return
	}

	args := CommitOrAbortArgs{TxID: tx.txID}
	var reply bool

	tx.connection.client.Call("ClientServer.Abort", &args, &reply)
	tx.cancelChan <- true
}

func (tx *KVTransaction) closeTX() {
	tx.finished = true
	tx.connection.client.Close()
	tx.connection.client = nil
}

func (c *KVConn) deleteDeadNodes(nodeIdxs []int) {
	fmt.Println("Deleteing dead nodes", nodeIdxs)
	var newNodeList []string
	for i, node := range c.serverNodes {
		deadNode := false
		for _, v := range nodeIdxs {
			if i == v {
				deadNode = true
			}
		}

		if !deadNode {
			newNodeList = append(newNodeList, node)
		}
	}
	c.serverNodes = newNodeList
}
