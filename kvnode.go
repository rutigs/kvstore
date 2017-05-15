/*
Usage:
go run kvnode.go [nodesFile] [nodeID] [listenNodeIn IP:port] [listenClientIn IP:port]
*/

package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// How long to wait for network operation
const timeoutDuration = 2000 * time.Millisecond

// How long to wait between heartbeats
const heartbeatDuration = 200 * time.Millisecond //heartbeat 5 times a second

// server type (for RPC)
type ClientServer int
type NodeServer int

type BroadcastStateRequest struct {
	History     map[int]int
	LatestItems map[string]string
	LastTID     int
	LastCID     int
}

type Node struct {
	nodeIP     string
	nodeClient *rpc.Client
}

type Nodes struct {
	sync.RWMutex
	List []Node // List of nodes in the system, where List[0] will be the master
}

type Transaction struct {
	sync.RWMutex
	KeyLock chan bool
	TxID    int
	Cache   map[string]string
}

type Clients struct {
	sync.RWMutex
	Map map[int]Transaction // map of txID -> Transaction
}

type KVStore struct {
	sync.RWMutex
	History      map[int]int       // map of txid -> cid
	Items        map[string]string // actual store of items
	LockedKeys   map[string]int    // map of key -> txid
	KeyLockQueue map[string][]int  // map of key -> List of txid
	Dependency   map[int]int       // map of txid -> txid
}

type ClientsStatus struct {
	sync.RWMutex
	Map map[int]time.Time // map of tXID -> last message from Transaction
}

var (
	//The IP and port of this node format "ip:port"
	externalIP string

	//Log output for this node
	logger *log.Logger

	// Map of nodes
	nodes Nodes

	// current Clients
	clients Clients

	// actual structure for holding kv pairs
	store KVStore

	// status of current clients
	clientsStatus ClientsStatus

	// lock to block operations until concensus
	initalConcensus sync.Once
)

func startup(nodeList []string) {

	logger = log.New(os.Stdout, "[Initalizing]", log.Lshortfile)

	nodes = Nodes{}
	clients = Clients{Map: make(map[int]Transaction)}

	store = KVStore{
		History:      make(map[int]int),
		Items:        make(map[string]string),
		Dependency:   make(map[int]int),
		KeyLockQueue: make(map[string][]int),
		LockedKeys:   make(map[string]int),
	}

	clientsStatus = ClientsStatus{Map: make(map[int]time.Time)}

	logger.Println(nodeList)

	nodes.Lock()
	for _, ip := range nodeList {

		var client *rpc.Client
		if ip != externalIP {
			client = connectServer(ip)
		}

		node := Node{
			nodeIP:     ip,
			nodeClient: client,
		}

		nodes.List = append(nodes.List, node)
		logger.Println("nodelist", nodes.List)
	}
	nodes.Unlock()

	logger = log.New(os.Stdout, "["+externalIP+"]", log.Lshortfile)
}

// Parses the command line arguments, two cases are valid
func ParseArguments() (nodesFile string, nodeID int, listenNodeIn string, listenClientIn string, err error) {
	args := os.Args[1:]
	if len(args) != 4 {
		err = fmt.Errorf("Please supply 4 command line arguments as seen in the spec http://www.cs.ubc.ca/~bestchai/teaching/cs416_2016w2/assign6/index.html")
		return
	}

	nodeID, err = strconv.Atoi(args[1])
	if err != nil {
		err = fmt.Errorf("unable to parse nodeID (argument 2) please supply an integer Error:%s", err.Error())
	}
	nodesFile = args[0]
	listenNodeIn = args[2]
	listenClientIn = args[3]
	return
}

// Main workhorse method.
func main() {

	// Parse the command line args, panic if error
	nodesFile, nodeID, listenNodeIn, listenClientIn, err := ParseArguments()
	if err != nil {
		panic(err)
	}

	nodeList, err := readLines(nodesFile)
	checkError("Reading NodesFile", err, false)

	externalIP = nodeList[nodeID-1]
	sort.Strings(nodeList)

	go func() {
		nServer := rpc.NewServer()
		n := new(NodeServer)
		nServer.Register(n)

		l, err := net.Listen("tcp", listenNodeIn)
		checkError("Error starting rpc server for nodes", err, true)
		logger.Println("NodeRPC server started")
		for {
			conn, err := l.Accept()
			checkError("Error accpecting node rpc requests", err, false)
			go nServer.ServeConn(conn)
		}
	}()

	startup(nodeList)

	go startNodeHeartbeat()
	go clientKeepAlive()
	go establishLeader()

	cServer := rpc.NewServer()
	c := new(ClientServer)
	cServer.Register(c)

	l, err := net.Listen("tcp", listenClientIn)
	checkError("Error starting rpc server for clients", err, true)
	logger.Println("ClientRPC server started")
	for {
		conn, err := l.Accept()
		checkError("Error accepting client rpc requests", err, false)
		go cServer.ServeConn(conn)
	}
}

func startNodeHeartbeat() {
	for {
		nodes.Lock()

		for nodeID, node := range nodes.List {

			if node.nodeIP == externalIP {
				continue
			}

			go func(ID int, node Node) {

				var reply bool
				timeout, err := callWithTimeout(node.nodeClient, "NodeServer.Heartbeat", externalIP, &reply, timeoutDuration)
				checkError("Error on Heartbeat", err, false)

				if timeout || err != nil {
					logger.Println("Node dead ", ID)

					nodes.Lock()
					nodes.List = removeNode(nodes.List, node.nodeIP)
					nodes.Unlock()

					go establishLeader()
				}

			}(nodeID, node)
		}
		nodes.Unlock()

		time.Sleep(heartbeatDuration)
	}
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

// CLIENT SERVER RPCS

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

func (n *ClientServer) Heartbeat(args *int, reply *bool) error {
	*reply = true
	clientsStatus.Lock()
	if _, ok := clientsStatus.Map[*args]; ok {
		clientsStatus.Map[*args] = time.Now()
	}
	clientsStatus.Unlock()
	//logger.Println("Heartbeat from client ", args, time.Now())
	return nil
}

func (n *ClientServer) NewTX(args *int, reply *int) error {

	initalConcensus.Do(establishConsensus)

	logger.Println("Received NewTX request")
	txID := getNewTXID()
	clients.Lock()
	newTX := Transaction{KeyLock: make(chan bool, 10), TxID: txID, Cache: make(map[string]string)}
	clients.Map[txID] = newTX
	clients.Unlock()
	clientsStatus.Lock()
	clientsStatus.Map[txID] = time.Now()
	clientsStatus.Unlock()
	*reply = txID
	return nil
}

type UUIDGenerator struct {
	sync.RWMutex
	lastID int
}

var txGen = UUIDGenerator{lastID: 0}

var commitIdGen = UUIDGenerator{lastID: 0}

func getNewTXID() int {
	txGen.Lock()
	defer txGen.Unlock()
	txGen.lastID = txGen.lastID + 1
	return txGen.lastID
}

func getNewCommitID() int {
	commitIdGen.Lock()
	defer commitIdGen.Unlock()
	commitIdGen.lastID = commitIdGen.lastID + 1
	return commitIdGen.lastID
}

type GetArgs struct {
	TxID int
	Key  string
}

type GetReply struct {
	Success bool
	Value   string
}

func (n *ClientServer) Get(args *GetArgs, reply *GetReply) error {

	initalConcensus.Do(establishConsensus)

	logger.Println("Received Get request for key:", args.Key)
	tx, err := getClient(args.TxID)
	if err != nil {
		reply.Success = false
		return err
	}

	tx.Lock()
	// First check if this transaction alreay has the item
	value, ok := tx.Cache[args.Key]
	if ok {
		//This transaction already locked this key before.
		reply.Value = value
		reply.Success = true
		tx.Unlock()
		return nil
	}

	tx.Unlock()
	tx.LockKey(args.Key)

	// deadlock may have aborted this transaction. Call getClient to check
	tx, err = getClient(args.TxID)
	if err != nil {
		reply.Success = false
		return err
	}

	store.Lock()
	tx.Lock()

	value, ok = store.Items[args.Key]
	tx.Cache[args.Key] = value
	if !ok {
		tx.Unlock()
		store.Unlock()
		reply.Success = false
		endTransaction(tx.TxID)
		return errors.New("Key Not Found")
	}

	tx.Unlock()
	store.Unlock()

	reply.Value = value
	reply.Success = true
	return nil
}

type PutArgs struct {
	TxID  int
	Key   string
	Value string
}

type PutReply struct {
	Success bool
}

func (n *ClientServer) Put(args *PutArgs, reply *PutReply) error {
	initalConcensus.Do(establishConsensus)

	logger.Println("Received Put request for key value:", args.Key, args.Value)
	tx, err := getClient(args.TxID)
	if err != nil {
		reply.Success = false
		return err
	}

	tx.Lock()
	// First check if this transaction alreay has the item
	_, ok := tx.Cache[args.Key]
	if ok {
		//This transaction already locked this key before.
		tx.Cache[args.Key] = args.Value
		reply.Success = true
		tx.Unlock()
		return nil
	}

	tx.Unlock()
	tx.LockKey(args.Key)

	// deadlock may have aborted this transaction. Call getClient to check
	tx, err = getClient(args.TxID)
	if err != nil {
		reply.Success = false
		return err
	}

	tx.Lock()
	tx.Cache[args.Key] = args.Value
	tx.Unlock()

	reply.Success = true
	return nil
}

type CommitOrAbortArgs struct {
	TxID int
}

func (n *ClientServer) Commit(args *CommitOrAbortArgs, reply *int) error {
	initalConcensus.Do(establishConsensus)

	logger.Println("Received Commit request with txID:", args.TxID)
	tx, err := getClient(args.TxID)
	if err != nil {
		*reply = 0
		return err
	}

	tx.Lock()
	store.Lock()
	for key, value := range tx.Cache {
		store.Items[key] = value
	}
	commitID := getNewCommitID()
	store.History[args.TxID] = commitID
	*reply = commitID
	store.Unlock()
	tx.Unlock()

	endTransaction(args.TxID)
	broadcastState()
	return nil
}

func (n *ClientServer) ConfirmCommit(args *CommitOrAbortArgs, reply *int) error {
	initalConcensus.Do(establishConsensus)

	store.Lock()
	defer store.Unlock()

	logger.Println("Received ConfirmCommit for txID:", args.TxID)
	if commitID, ok := store.History[args.TxID]; ok {
		*reply = commitID
		return nil
	}

	*reply = -1
	return nil
}

func (n *ClientServer) Abort(args *CommitOrAbortArgs, reply *bool) error {
	initalConcensus.Do(establishConsensus)

	logger.Println("Received Abort for txID:", args.TxID)
	endTransaction(args.TxID)
	*reply = true
	return nil
}

func getClient(txID int) (*Transaction, error) {
	clients.Lock()
	defer clients.Unlock()
	tx, ok := clients.Map[txID]
	if !ok {
		return &Transaction{}, errors.New("Missing client for that transaction ID!")
	}

	return &tx, nil
}

func clientKeepAlive() {

	for {
		clientsStatus.Lock()
		for txID, lastHearbeat := range clientsStatus.Map {
			if time.Since(lastHearbeat) > timeoutDuration {
				logger.Println("Client died, txID: ", txID)
				go endTransaction(txID)
			}
		}
		clientsStatus.Unlock()
		time.Sleep(timeoutDuration)
	}
}

func endTransaction(txID int) {
	logger.Println("Ending Transaction", txID)
	tx, err := getClient(txID)
	checkError("Ending Transaction", err, false)
	if err != nil {
		return
	}

	logger.Println("Ending Transaction", txID, "got client")

	store.Lock()
	tx.Lock()
	logger.Println("Ending Transaction", txID, "goint to unlock keys")

	for key, _ := range tx.Cache {
		tx.UnlockKey(key)
	}
	logger.Println("Ending Transaction", txID, "released all locks")
	delete(store.Dependency, txID)
	tx.Unlock()
	store.Unlock()

	clients.Lock()
	if _, ok := clients.Map[txID]; ok {
		delete(clients.Map, txID)
	}
	clients.Unlock()

	clientsStatus.Lock()
	if _, ok := clientsStatus.Map[txID]; ok {
		delete(clientsStatus.Map, txID)
	}
	clientsStatus.Unlock()

	logger.Println("Transaction ended. store:", store.LockedKeys, store.KeyLockQueue, store.Dependency)
	logger.Println("Transaction ended. client:", clients.Map, clientsStatus.Map)
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

// NODE SERVER RPCS

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

func (n *NodeServer) Heartbeat(nodeIP string, reply *bool) error {
	*reply = true
	//logger.Println("Heartbeat at from peer", nodeIP, time.Now())
	return nil
}

func (n *NodeServer) QueryLatestCommitID(nodeIP string, reply *int) error {

	store.Lock()
	*reply = len(store.History)
	store.Unlock()

	logger.Println("QueryLatestCommitID done for ", nodeIP, " CID is: ", *reply)
	return nil
}

func (n *NodeServer) UpdateState(newState BroadcastStateRequest, reply *bool) error {

	store.Lock()
	store.Items = newState.LatestItems
	store.History = newState.History
	store.Unlock()

	txGen.lastID = newState.LastTID
	commitIdGen.lastID = newState.LastCID

	*reply = true
	logger.Println("UpdateState done for ", externalIP)
	return nil
}

func (n *NodeServer) BroadcastState(nodeIP string, reply *bool) error {
	broadcastState()
	logger.Println("BroadcastState done for ", nodeIP)
	return nil
}

func broadcastState() {
	var wg sync.WaitGroup

	store.Lock()
	broadcast := BroadcastStateRequest{
		History:     store.History,
		LatestItems: store.Items,
		LastCID:     txGen.lastID,
		LastTID:     commitIdGen.lastID,
	}
	store.Unlock()

	nodes.Lock()

	if len(nodes.List) == 1 {
		return
	}

	for nodeID, node := range nodes.List {

		if node.nodeIP == externalIP {
			continue
		}
		wg.Add(1)

		go func(ID int, nodeClient *rpc.Client) {
			defer wg.Done()

			var reply bool
			_, err := callWithTimeout(nodeClient, "NodeServer.UpdateState", broadcast, &reply, timeoutDuration)
			checkError("Error calling UpdateState", err, false)

		}(nodeID, node.nodeClient)
	}
	nodes.Unlock()

	wg.Wait()
}

func tryEstablishConsensus() bool {

	if len(nodes.List) == 1 {
		logger.Println("Last Node left: Reached consensus.")
		return true
	}

	var wg sync.WaitGroup

	var consensusMutex sync.Mutex
	consensus := make(map[int]*rpc.Client)

	nodes.Lock()
	for nodeID, node := range nodes.List {

		if node.nodeIP == externalIP {
			consensus[len(store.History)] = nil
			continue
		}
		wg.Add(1)

		go func(ID int, nodeClient *rpc.Client) {
			defer wg.Done()

			var cID int
			timeout, err := callWithTimeout(nodeClient, "NodeServer.QueryLatestCommitID", externalIP, &cID, timeoutDuration)
			checkError("Error on QueryLatestTID", err, false)

			if !timeout && err == nil {
				consensusMutex.Lock()
				consensus[cID] = nodeClient
				consensusMutex.Unlock()
				logger.Println("consensus:", consensus)
			}
		}(nodeID, node.nodeClient)
	}
	nodes.Unlock()

	wg.Wait()

	if len(consensus) <= 1 {
		logger.Println("Reached consensus.")
		return true
	}

	var latestCID int
	for cID, _ := range consensus {
		if cID > latestCID {
			latestCID = cID

		}
	}

	logger.Println("consensus:", consensus, "latestCID", latestCID)

	if consensus[latestCID] != nil {
		var reply bool
		timeout, err := callWithTimeout(consensus[latestCID], "NodeServer.BroadcastState", externalIP, &reply, 5*timeoutDuration)
		checkError("Error on BroadcastState", err, false)

		if timeout || err != nil {
			return false
		} else {
			return true
		}
	} else {
		broadcastState()
		return true
	}
}

// establishes the leader node
func establishLeader() {
	nodes.Lock()
	logger.Println("Establishing leader:", nodes.List, externalIP)
	leader := nodes.List[0]
	nodes.Unlock()
	if leader.nodeIP == externalIP {
		logger.Println("This node is leader.")
		initalConcensus.Do(establishConsensus)
	}

}

func establishConsensus() {
	ok := false
	for !ok {
		ok = tryEstablishConsensus()
	}
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

// Key Lock Management

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

func (tx *Transaction) LockKey(key string) {
	logger.Println("Locking Key", key, tx.TxID)
	newOwner := tx.TxID
	store.Lock()
	owner, ok := store.LockedKeys[key]
	if !ok || owner == newOwner {
		// Aquired lock
		logger.Println("No Owner for lock, aquired lock", key, tx.TxID)
		store.LockedKeys[key] = newOwner
		store.Unlock()
		return
	}

	// Queue for lock
	store.Dependency[newOwner] = owner
	queue, _ := store.KeyLockQueue[key]
	store.KeyLockQueue[key] = append(queue, newOwner)

	logger.Println("Locking Key", key, tx.TxID, "owner", owner, "queue:", queue)

	next := owner
	txToAbort := tx
	deadlock := false
	for ok {
		logger.Println("Checking for deadlock")
		currentTx, err := getClient(next)
		if err == nil {
			if len(currentTx.Cache) < len(txToAbort.Cache) {
				txToAbort = currentTx
			}
		}
		next, ok = store.Dependency[next]
		if next == newOwner {
			deadlock = true
			break
		}
	}

	store.Unlock()

	if deadlock {
		logger.Println("Locking Key", key, tx.TxID, "deadlocked, aborting:", txToAbort.TxID)
		endTransaction(txToAbort.TxID)
		if txToAbort.TxID == tx.TxID {
			return
		}
	}

	// Wait for Unlock to unlock transaction
	logger.Println("Queuein for lock", key, tx.TxID)
	<-tx.KeyLock
	logger.Println("Got for lock", key, tx.TxID)

}

func (tx *Transaction) UnlockKey(key string) {
	logger.Println("Unlocking Key", key)

	owner, ok := store.LockedKeys[key]
	if !ok {
		logger.Println("Inconsistant state during unlock, key not found", key)
		return
	}
	delete(store.LockedKeys, key)

	// Handoff lock key to next in queue
	for queue, ok := store.KeyLockQueue[key]; ok && len(queue) > 0; queue, ok = store.KeyLockQueue[key] {
		logger.Println("Unlocking Key", key, "queue is", queue)
		newOwner, newQueue := queue[0], queue[1:]
		store.KeyLockQueue[key] = newQueue

		newOwnerTx, err := getClient(newOwner)
		if err != nil {
			checkError("Dead transaction encountered unlock, new owner transaction not found ", err, false)
			continue
		}

		logger.Println("Unlocking Key", key, "Adjust Dependency", store.Dependency)
		store.LockedKeys[key] = newOwner
		delete(store.Dependency, newOwner)

		for tx, waitingFor := range store.Dependency {
			if waitingFor == owner {
				store.Dependency[tx] = newOwner
			}
		}
		logger.Println("Unlocking newowner", key, newOwner, store.Dependency)
		newOwnerTx.KeyLock <- true
		return
	}
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

// Utility functions

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

func removeNode(nodeList []Node, ip string) []Node {
	var result []Node
	logger.Println("nodeList", nodeList, "ip", ip)

	for _, node := range nodeList {
		if node.nodeIP != ip {
			result = append(result, node)
		}
	}

	logger.Println("result", result)
	return result
}

// readLines reads a whole file into memory
// and returns a slice of its lines.
func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

// dial and connect to server
func connectServer(server_ip_port string) *rpc.Client {
	client, err := rpc.Dial("tcp", server_ip_port)
	checkError("connecting to server", err, false)

	for err != nil {
		logger.Println("Retrying connection")
		time.Sleep(time.Second)
		client, err = rpc.Dial("tcp", server_ip_port)
		checkError("connecting to server", err, false)
	}

	logger.Println("Conneceted to server: ", server_ip_port)
	return client
}

func checkError(msg string, err error, exit bool) {
	if err != nil {
		logger.Println(msg, err)
		if exit {
			os.Exit(0)
		}
	}
}

// Call a RPC function with timeout
func callWithTimeout(client *rpc.Client, serviceMethod string, args interface{}, reply interface{}, timeout time.Duration) (bool, error) {
	call := client.Go(serviceMethod, args, reply, nil)
	select {
	case <-call.Done:
		return false, call.Error
	case <-time.After(timeout):
		return true, nil
	}
}
