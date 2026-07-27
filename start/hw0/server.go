package main

import (
	"fmt"
	"net"
)

func main() {
	message := "OK\n"
	listener, err := net.Listen("tcp", ":8080")

	if err != nil {
		fmt.Println(err)
		return
	}
	defer listener.Close()

	fmt.Println("Listening on port 8080")
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}

		go handleConnection(conn, message)
	}
}


func handleConnection(conn net.Conn, message string) {
	defer conn.Close()

	_, err := conn.Write([]byte(message))
	if err != nil {
		fmt.Println("Write error:",err)
		return
	}
}