package main

import (
	"fmt"
	"log"
	"os"
	"time"
)

type Node struct {
	name        string
	stateM      map[string]int
	commitIndex int
	lastApplied int
	logs        []string
	peers       map[string]*Node
	matchIdx    map[string]int
	nextIdx     map[string]int
	AppendCh    chan Append
	MsgCh       chan Msg
}

type Msg struct {
	command string
}
type Append struct {
	prevLogIndex int
	entries      []string
	nodeName     string
	leaderCommit int
}
type AppendRes struct {
	success  bool
	matchIdx int
	nodeName string
}

func process(n Node) {
	path := fmt.Sprint(n.name, ".log")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	for {
		select {
		case msg := <-n.MsgCh:
			f.WriteString(msg.command)

			// it needs to broadcast appends to the follower
			for _, peer := range n.peers {
				app := Append{
					val:  res.command,
					kind: "request",
				}
				peer.AppendCh <- app
			}
		case app := <-n.AppendCh:

			// we do logic when we recieve from append Channel
			// print it out
			fmt.Println(app.val, "was recieved by follower: ", n.name)

			// log it to their file
			f.WriteString(app.val)
		}

	}

}

func main() {
	nodeA := Node{
		name:        "A",
		stateM:      make(map[string]int, 0),
		commitIndex: 0,
		lastApplied: 0,
		logs:        make([]string, 0),
		matchIdx:    make(map[string]int, 0),
		nextIdx:     make(map[string]int, 0),
		AppendCh:    make(chan Append, 10),
		MsgCh:       make(chan Msg, 10),
		peers:       make(map[string]*Node),
	}

	nodeB := Node{
		name:        "B",
		stateM:      make(map[string]int, 0),
		commitIndex: 0,
		lastApplied: 0,
		logs:        make([]string, 0),
		matchIdx:    make(map[string]int, 0),
		nextIdx:     make(map[string]int, 0),
		AppendCh:    make(chan Append, 10),
		MsgCh:       make(chan Msg, 10),
		peers:       make(map[string]*Node),
	}

	nodeA.peers["B"] = &nodeB
	nodeB.peers["A"] = &nodeA

	// instantiate the leader state for nodeA
	nodeA.matchIdx["B"] = 0
	nodeA.nextIdx["B"] = 0

	// start the nodes up
	go process(nodeA)
	go process(nodeB)

	// send a message to leader
	nodeA.MsgCh <- Msg{command: "SET x=1"}

	time.Sleep(3000 * time.Millisecond)
}
