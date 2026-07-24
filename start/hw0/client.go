package main

import (
	"fmt"
	"net"
)

func main() {
	expectedMessage := "OK\n"
	conn, err := net.Dial("tcp", "127.0.0.1:8080")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()

	input := make([]byte, 1024)
	n, err := conn.Read(input)
	if n == 0 || err != nil {
		fmt.Println("Read error:", err)
		return
	}
	source := string(input[:n])

	if source == expectedMessage {
		fmt.Println("\nExpected message received! =)")
	} else {
		fmt.Println("\nWas expecting:", expectedMessage, "but got:", source)
	}
}
