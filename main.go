package main

import (
	"log"
	"net"
	"time"
)

type Peer struct {
	Addr     *net.UDPAddr
	LastSeen time.Time
}

var peers = make(map[string]*Peer)

func main() {
	addr, err := net.ResolveUDPAddr("udp", ":9000")
	if err != nil {
		log.Fatal(err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	buffer := make([]byte, 1024)
	log.Println("Relay server started on :9000")

	for {
		n, clientAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Println("Read error:", err)
			continue
		}

		msg := buffer[:n]
		log.Printf("Received %d bytes from %s", n, clientAddr)

		key := clientAddr.String()
		peers[key] = &Peer{Addr: clientAddr, LastSeen: time.Now()}

		// Forward to other peers (basic relay logic)
		for peerKey, peer := range peers {
			if peerKey != key {
				conn.WriteToUDP(msg, peer.Addr)
				log.Printf("Forwarded message from %s to %s", key, peerKey)
			}
		}
	}
}
