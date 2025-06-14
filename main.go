// ================================
// 🌐 GO RELAY SERVER (TCP VERSION)
// ================================

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
)

type Client struct {
	Conn   net.Conn
	IsHost bool
	ID     string
}

type Message struct {
	Type   string `json:"type"` // create / join / message
	RoomID string `json:"room"`
	Msg    string `json:"msg"`
	From   string `json:"from"`
}

type Room struct {
	Host  *Client
	Peers map[string]*Client
	Mutex sync.Mutex
}

var rooms = make(map[string]*Room)
var roomMutex sync.Mutex

func main() {
	ln, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Relay Server running on port 9000")

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	client := &Client{Conn: conn, ID: conn.RemoteAddr().String()}

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return
		}

		var msg Message
		if err := json.Unmarshal(line, &msg); err != nil {
			continue
		}

		switch msg.Type {
		case "create":
			createRoom(msg.RoomID, client)
		case "join":
			joinRoom(msg.RoomID, client)
		case "message":
			forwardMessage(msg, client)
		}
	}
}

func createRoom(roomID string, host *Client) {
	roomMutex.Lock()
	defer roomMutex.Unlock()

	host.IsHost = true
	rooms[roomID] = &Room{
		Host:  host,
		Peers: make(map[string]*Client),
	}
	fmt.Println("Room created:", roomID)
}

func joinRoom(roomID string, client *Client) {
	roomMutex.Lock()
	room, exists := rooms[roomID]
	roomMutex.Unlock()
	if !exists {
		return
	}
	room.Mutex.Lock()
	room.Peers[client.ID] = client
	room.Mutex.Unlock()

	fmt.Println("Client joined room:", roomID)
	room.Host.Conn.Write([]byte(fmt.Sprintf("{\"type\":\"join\",\"from\":\"%s\"}\n", client.ID)))
}

func forwardMessage(msg Message, sender *Client) {
	roomMutex.Lock()
	room, exists := rooms[msg.RoomID]
	roomMutex.Unlock()
	if !exists {
		return
	}

	data, _ := json.Marshal(msg)
	data = append(data, '\n')

	if sender.IsHost {
		room.Mutex.Lock()
		for _, peer := range room.Peers {
			peer.Conn.Write(data)
		}
		room.Mutex.Unlock()
	} else {
		room.Host.Conn.Write(data)
	}
}
