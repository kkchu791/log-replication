package main

import (
	"fmt"
	"log"
	"os"
	"time"
)

func process(msgCh <-chan string) {
	res := <-msgCh
	fmt.Println(res)
	path := "A.log"
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	f.WriteString(res)
}

func main() {
	msgCh := make(chan string)
	go process(msgCh)
	msgCh <- "beep"
	time.Sleep(1000 * time.Millisecond)
}
