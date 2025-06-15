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
	"time"
)

// Client represents a connected client with its connection, host status, and ID.
type Client struct {
	Conn   net.Conn
	IsHost bool
	ID     string
}

// Message defines the structure of messages exchanged between client and server.
type Message struct {
	Type   string `json:"type"` // "create", "join", "message"
	RoomID string `json:"room"` // The ID of the room
	Msg    string `json:"msg"`  // The actual message content
	From   string `json:"from"` // The sender's ID (typically client.ID)
}

// Room represents a chat room with a host and multiple peers.
type Room struct {
	Host  *Client
	Peers map[string]*Client // Map of client IDs to Client pointers
	Mutex sync.Mutex         // Mutex to protect access to Peers map
}

// Global maps to store active rooms and a mutex to protect it.
var rooms = make(map[string]*Room)
var roomMutex sync.Mutex

func main() {
	// Listen for incoming TCP connections on port 9000.
	ln, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("Failed to start server: %v", err) // Use Fatalf to exit on critical error
	}
	fmt.Println("Relay Server running on port 9000")

	// Accept new connections in a loop.
	for {
		conn, err := ln.Accept()
		if err != nil {
			// Log non-fatal accept errors and continue.
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		// Handle each client connection in a new goroutine.
		go handleClient(conn)
	}
}

// handleClient manages the lifecycle and message processing for a single client.
func handleClient(conn net.Conn) {
	// Ensure the connection is closed when the function exits.
	defer func() {
		fmt.Printf("%s Client disconnected: %s\n", time.Now().Format(time.TimeOnly), conn.RemoteAddr().String())
		conn.Close()
	}()

	reader := bufio.NewReader(conn)
	client := &Client{Conn: conn, ID: conn.RemoteAddr().String()}
	fmt.Printf("%s New client connected: %s\n", time.Now().Format(time.TimeOnly), client.ID)

	// Loop to continuously read messages from the client.
	for {
		// Read bytes until a newline character is encountered.
		line, err := reader.ReadBytes('\n')
		if err != nil {
			log.Printf("Error reading from client %s: %v\n", client.ID, err)
			// If there's a read error (e.g., client disconnected), break the loop.
			return
		}

		// --- Debugging additions ---
		fmt.Printf("%s Received raw message from %s: %s", time.Now().Format(time.TimeOnly), client.ID, string(line))

		var msg Message
		// Unmarshal the JSON message into the Message struct.
		if err := json.Unmarshal(line, &msg); err != nil {
			log.Printf("Error unmarshalling JSON from %s: %v. Raw: %q\n", client.ID, err, string(line))
			// If JSON unmarshalling fails, skip to the next message.
			continue
		}

		// --- Debugging additions ---
		fmt.Printf("%s Parsed message from %s: Type=%q, RoomID=%q, From=%q\n",
			time.Now().Format(time.TimeOnly), client.ID, msg.Type, msg.RoomID, msg.From)

		// Process the message based on its type.
		switch msg.Type {
		case "create":
			createRoom(msg.RoomID, client)
			// This print statement is now supplemented by logs within createRoom
			fmt.Printf("%s handleClient: Processed 'create' for room %q\n", time.Now().Format(time.TimeOnly), msg.RoomID)
		case "join":
			joinRoom(msg.RoomID, client)
			fmt.Printf("%s handleClient: Processed 'join' for room %q\n", time.Now().Format(time.TimeOnly), msg.RoomID)
		case "message":
			forwardMessage(msg, client)
			fmt.Printf("%s handleClient: Processed 'message' for room %q\n", time.Now().Format(time.TimeOnly), msg.RoomID)
		default:
			fmt.Printf("%s handleClient: Received unknown message type %q from %s\n", time.Now().Format(time.TimeOnly), msg.Type, client.ID)
		}
	}
}

// createRoom attempts to create a new room with the given roomID and host.
func createRoom(roomID string, host *Client) {
	roomMutex.Lock() // Lock to protect the global rooms map
	defer roomMutex.Unlock()

	// Check if the room already exists.
	if _, exists := rooms[roomID]; exists {
		fmt.Printf("%s Room '%s' already exists. Client %s tried to create it again.\n",
			time.Now().Format(time.TimeOnly), roomID, host.ID)
		// Optionally, send an error message back to the client.
		host.Conn.Write([]byte(fmt.Sprintf("{\"type\":\"error\",\"msg\":\"Room %s already exists\"}\n", roomID)))
		return // Exit if room already exists.
	}

	host.IsHost = true // Mark the client as the host of this new room.
	rooms[roomID] = &Room{
		Host:  host,
		Peers: make(map[string]*Client), // Initialize an empty map for peers.
	}
	fmt.Printf("%s Room '%s' successfully created by host %s.\n",
		time.Now().Format(time.TimeOnly), roomID, host.ID)

	// Send a confirmation message back to the host.
	host.Conn.Write([]byte(fmt.Sprintf("{\"type\":\"success\",\"msg\":\"Room %s created successfully\"}\n", roomID)))
}

// joinRoom allows a client to join an existing room.
func joinRoom(roomID string, client *Client) {
	roomMutex.Lock()
	room, exists := rooms[roomID]
	roomMutex.Unlock() // Unlock after accessing the global rooms map.

	if !exists {
		fmt.Printf("%s Client %s tried to join non-existent room: %s\n",
			time.Now().Format(time.TimeOnly), client.ID, roomID)
		// Optionally, send an error back to the client.
		client.Conn.Write([]byte(fmt.Sprintf("{\"type\":\"error\",\"msg\":\"Room %s does not exist\"}\n", roomID)))
		return
	}

	room.Mutex.Lock() // Lock the specific room's mutex to protect its peers map.
	room.Peers[client.ID] = client
	room.Mutex.Unlock()

	fmt.Printf("%s Client %s joined room: %s\n", time.Now().Format(time.TimeOnly), client.ID, roomID)

	// Notify the host that a new client has joined.
	hostJoinMsg := fmt.Sprintf("{\"type\":\"join_notification\",\"from\":\"%s\",\"room\":\"%s\"}\n", client.ID, roomID)
	room.Host.Conn.Write([]byte(hostJoinMsg))
	fmt.Printf("%s Sent join notification to host %s for new peer %s in room %s\n",
		time.Now().Format(time.TimeOnly), room.Host.ID, client.ID, roomID)

	// Send a success message back to the joining client.
	client.Conn.Write([]byte(fmt.Sprintf("{\"type\":\"success\",\"msg\":\"Joined room %s\"}\n", roomID)))
}

// forwardMessage relays messages between the host and peers within a room.
func forwardMessage(msg Message, sender *Client) {
	roomMutex.Lock()
	room, exists := rooms[msg.RoomID]
	roomMutex.Unlock()

	if !exists {
		fmt.Printf("%s Message from %s for non-existent room: %s\n",
			time.Now().Format(time.TimeOnly), sender.ID, msg.RoomID)
		// Optionally, send an error back to the sender.
		sender.Conn.Write([]byte(fmt.Sprintf("{\"type\":\"error\",\"msg\":\"Room %s does not exist\"}\n", msg.RoomID)))
		return
	}

	// Marshal the message back to JSON bytes, ensuring it ends with a newline.
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Error marshalling message for forwarding: %v\n", err)
		return
	}
	data = append(data, '\n')

	if sender.IsHost {
		// If the sender is the host, forward the message to all peers.
		room.Mutex.Lock()
		for _, peer := range room.Peers {
			if peer.Conn != sender.Conn { // Don't send back to the sender (host)
				_, writeErr := peer.Conn.Write(data)
				if writeErr != nil {
					log.Printf("Error writing to peer %s: %v\n", peer.ID, writeErr)
					// Handle peer disconnection (e.g., remove from room) - beyond current scope
				} else {
					fmt.Printf("%s Forwarded message from host %s to peer %s in room %s\n",
						time.Now().Format(time.TimeOnly), sender.ID, peer.ID, msg.RoomID)
				}
			}
		}
		room.Mutex.Unlock()
	} else {
		// If the sender is a peer, forward the message to the host.
		_, writeErr := room.Host.Conn.Write(data)
		if writeErr != nil {
			log.Printf("Error writing to host %s: %v\n", room.Host.ID, writeErr)
			// Handle host disconnection - beyond current scope
		} else {
			fmt.Printf("%s Forwarded message from peer %s to host %s in room %s\n",
				time.Now().Format(time.TimeOnly), sender.ID, room.Host.ID, msg.RoomID)
		}
	}
}
