package main

import "fmt"

func process(msgCh <-chan string) {
	res := <-msgCh
	fmt.Println(res)
}

func main() {

	msgCh := make(chan string)
	go process(msgCh)
	msgCh <- "beep"
}
