/*
Simple echo server written in Go.

It keeps a pool of handlers (MAX_HANDLERS) open to service incoming connections.

The MAX_QUEUE size governs the maximum number of connections to queue before they
are serviced by handlers.
*/
package main

import (
	"fmt"
	"io"
	"net"
)

const PORT = 8118
const MAX_HANDLERS = 10
const MAX_QUEUE = 100

// Pulls a connection from the queue to echo the data back at the client.  It closes the connection
// as soon as it receives an EOF from the incoming connection.
func handleConnection(chanBuffer chan net.Conn) {
	var bytesEchoed int64 = 0
	for conn := range chanBuffer {
		written, err := io.Copy(conn, conn)
		if err != nil {
			fmt.Println("Err echo: " + err.Error())
			continue
		}
		bytesEchoed += written
		conn.Close()
		fmt.Printf("Connection closed - echoed %d bytes\n", bytesEchoed)
	}
}

func main() {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", PORT))
	defer ln.Close()
	if err != nil {
		panic("Err listen: " + err.Error())
	}
	fmt.Println("Listening on port: ", PORT)
	chanBuffer := make(chan net.Conn, MAX_QUEUE)
	fmt.Println("Request queue size: ", MAX_QUEUE)
	// Create the fixed pool of handlers to service connections
	for i := 0; i < MAX_HANDLERS; i++ {
		go handleConnection(chanBuffer)
		fmt.Println("Creating connection handler: ", i+1)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Err accept: " + err.Error())
			continue
		}
		fmt.Println("Connection opened...")
		// Push an accepted connection into the connection queue.
		// If the buffer is full (> MAX_QUEUE) this will block until space is available.
		chanBuffer <- conn
		fmt.Println("Queue size: ", len(chanBuffer))
	}
}
