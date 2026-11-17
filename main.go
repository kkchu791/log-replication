package main

import (
	"fmt"
	"log"
	"os"
	"time"
)

type Node struct {
	name     string
	AppendCh chan Append
	MsgCh    chan Msg
	peers    map[string]Node
}

type Msg struct {
	val string
}
type Append struct {
	val     string
	kind    string
	success bool
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
		case res := <-n.MsgCh:
			fmt.Println(res)
			f.WriteString(res.val)

			// it needs to broadcast appends to the follower
			for _, peer := range n.peers {
				app := Append{
					val:  res.val,
					kind: "request",
				}
				peer.AppendCh <- app
			}
		case app := <-n.AppendCh:

			// we do logic when we recieve from append Channel
			// print it out
			fmt.Println(app.val, "was recieved by follower: ", n.name)
		}

	}

}

func main() {

	nodeA := Node{
		name:     "A",
		AppendCh: make(chan Append, 10),
		MsgCh:    make(chan Msg, 10),
		peers:    make(map[string]Node),
	}

	nodeB := Node{
		name:     "B",
		AppendCh: make(chan Append, 10),
		MsgCh:    make(chan Msg, 10),
		peers:    make(map[string]Node),
	}

	nodeA.peers["B"] = nodeB
	nodeB.peers["A"] = nodeA

	go process(nodeA)
	go process(nodeB)
	nodeA.MsgCh <- Msg{val: "beep"}
	time.Sleep(3000 * time.Millisecond)
}
