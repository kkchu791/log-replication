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
	AppendResCh chan AppendRes
	MsgCh       chan Msg
}

type Msg struct {
	command string
}
type Append struct {
	prevLogIdx   int
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
			//write to logfile
			f.WriteString(msg.command)

			// write to logs to in-memory copy
			n.logs = append(n.logs, msg.command)

			// it needs to broadcast appends to the follower
			for _, peer := range n.peers {

				nextIdx := n.nextIdx[peer.name]
				prevLogIdx := nextIdx - 1

				entries := n.logs[nextIdx-1:]

				app := Append{
					prevLogIdx:   prevLogIdx,
					entries:      entries,
					nodeName:     n.name,
					leaderCommit: n.commitIndex,
				}
				peer.AppendCh <- app
			}
		case app := <-n.AppendCh:
			var success bool
			if app.prevLogIdx <= len(n.logs) {
				success = true

				for _, entry := range app.entries {
					fmt.Println(entry, "was recieved by follower: ", n.name)

					// log it to their file
					f.WriteString(entry)

					// update the follower state
					n.logs = append(n.logs, entry)
				}

				// sends back a append res
				leader := n.peers[app.nodeName]

				matchIdx := app.prevLogIdx + len(app.entries)
				appRes := AppendRes{
					success:  success,
					nodeName: n.name,
					matchIdx: matchIdx,
				}

				fmt.Println("appRes: ", appRes)
				leader.AppendResCh <- appRes
			} else {

				success = false
				fmt.Println("need to write logic for if if the leader overshot the estimate")
			}
		case appRes := <-n.AppendResCh:
			fmt.Println(n.name, " got this append Res: ", appRes)
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
		AppendResCh: make(chan AppendRes, 10),
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
		AppendResCh: make(chan AppendRes, 10),
		MsgCh:       make(chan Msg, 10),
		peers:       make(map[string]*Node),
	}

	nodeA.peers["B"] = &nodeB
	nodeB.peers["A"] = &nodeA

	// instantiate the leader state for nodeA
	nodeA.matchIdx["B"] = 0
	nodeA.nextIdx["B"] = 1

	// start the nodes up
	go process(nodeA)
	go process(nodeB)

	// send a message to leader
	nodeA.MsgCh <- Msg{command: "SET x=1"}

	time.Sleep(5000 * time.Millisecond)
}
