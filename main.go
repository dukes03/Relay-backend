package main

import (
	"fmt"
	"io"
	"log"
	"net"
)

func handleConnection(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, 1024)

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			if err != io.EOF {
				log.Println("Read error:", err)
			}
			return
		}

		data := buffer[:n]
		fmt.Printf("Received: %s\n", data)

		// Echo กลับไปที่ client
		_, err = conn.Write([]byte("Relay: " + string(data)))
		if err != nil {
			log.Println("Write error:", err)
			return
		}
	}
}

func main() {
	ln, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatal("Listen error:", err)
	}
	log.Println("Relay server started on :9000")

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("Accept error:", err)
			continue
		}
		go handleConnection(conn)
	}
}
