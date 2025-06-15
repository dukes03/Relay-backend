package main

import (
	"log"
	"net"
	"sync"
	"time"
)

type Peer struct {
	Addr     *net.UDPAddr
	LastSeen time.Time
}

var (
	peers     = make(map[string]*Peer)
	peersLock sync.Mutex
)

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

	buffer := make([]byte, 2048)
	log.Println("Relay server started on :9000")

	for {
		n, clientAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Println("Read error:", err)
			continue
		}

		msg := buffer[:n]
		registerPeer(clientAddr)

		log.Printf("Message from %s: %s", clientAddr, string(msg))

		// Relay to all other peers
		relayToOthers(conn, msg, clientAddr)
	}
}

func registerPeer(addr *net.UDPAddr) {
	peersLock.Lock()
	defer peersLock.Unlock()
	key := addr.String()
	if _, exists := peers[key]; !exists {
		log.Println("New peer registered:", key)
	}
	peers[key] = &Peer{Addr: addr, LastSeen: time.Now()}
}

func relayToOthers(conn *net.UDPConn, msg []byte, sender *net.UDPAddr) {
	peersLock.Lock()
	defer peersLock.Unlock()
	for key, peer := range peers {
		if key != sender.String() {
			_, err := conn.WriteToUDP(msg, peer.Addr)
			if err != nil {
				log.Printf("Failed to send to %s: %v", key, err)
			} else {
				log.Printf("Forwarded from %s to %s", sender, key)
			}
		}
	}
}
