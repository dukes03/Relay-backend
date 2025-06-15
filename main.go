package main

import (
	"encoding/binary"
	"log"
	"net"
	"sync"
	"time"
)

const (
	PORT        = ":7777" // Port for the relay server to listen on
	BUFFER_SIZE = 5000    // Maximum size of data packets (including our header)

	// Message Types for internal relay communication
	MsgTypeFishNetData        byte = 0 // Regular FishNet data
	MsgTypeNewClientConnected byte = 1 // Notification that a new client connected to the relay
	MsgTypeClientDisconnected byte = 2 // Notification that a client disconnected from the relay
)

// Client represents a connected Unity client
type Client struct {
	ID   int // Unique ID assigned by the relay server
	Conn net.Conn
}

// RelayServer holds the state of the server
type RelayServer struct {
	clients      map[int]*Client
	nextClientID int
	mu           sync.Mutex // Mutex to protect clients map
	register     chan *Client
	unregister   chan *Client
	broadcast    chan []byte // Channel for broadcasting messages with headers
}

// NewRelayServer creates and returns a new RelayServer instance
func NewRelayServer() *RelayServer {
	return &RelayServer{
		clients:      make(map[int]*Client),
		nextClientID: 1, // Start client IDs from 1
		register:     make(chan *Client),
		unregister:   make(chan *Client),
		broadcast:    make(chan []byte),
	}
}

// Run starts the relay server
func (rs *RelayServer) Run() {
	listener, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", PORT, err)
	}
	defer listener.Close()
	log.Printf("Relay server listening on %s", PORT)

	go rs.manageClients() // Start goroutine to manage client connections

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		rs.mu.Lock()
		client := &Client{
			ID:   rs.nextClientID,
			Conn: conn,
		}
		rs.nextClientID++
		rs.mu.Unlock()

		rs.register <- client // Register the new client
		go rs.handleClient(client)
	}
}

// manageClients handles registering, unregistering, and broadcasting messages
func (rs *RelayServer) manageClients() {
	for {
		select {
		case client := <-rs.register:
			rs.mu.Lock()
			rs.clients[client.ID] = client
			log.Printf("Client %d connected from %s. Total clients: %d", client.ID, client.Conn.RemoteAddr(), len(rs.clients))

			// Notify all existing clients about the new client (including the new client itself)
			for _, existingClient := range rs.clients {
				if existingClient.ID != client.ID { // Don't send "new connection" message to self, unless needed
					// Send to existing clients that 'client.ID' has connected
					message := make([]byte, 1+4) // MessageType + ID
					message[0] = MsgTypeNewClientConnected
					binary.BigEndian.PutUint32(message[1:], uint32(client.ID))
					go func(c net.Conn, msg []byte) {
						_, err := c.Write(msg)
						if err != nil {
							log.Printf("Error sending new client connected notification to %s: %v", c.RemoteAddr(), err)
						}
					}(existingClient.Conn, message)
				}
				// Also send "new connection" message for existing clients to the newly connected client
				// This informs the new client about who is already connected.
				message := make([]byte, 1+4)
				message[0] = MsgTypeNewClientConnected
				binary.BigEndian.PutUint32(message[1:], uint32(existingClient.ID))
				go func(c net.Conn, msg []byte) {
					_, err := c.Write(msg)
					if err != nil {
						log.Printf("Error sending existing client connected notification to new client %s: %v", c.RemoteAddr(), err)
					}
				}(client.Conn, message)
			}
			rs.mu.Unlock()

		case client := <-rs.unregister:
			rs.mu.Lock()
			if _, ok := rs.clients[client.ID]; ok {
				delete(rs.clients, client.ID)
				client.Conn.Close()
				log.Printf("Client %d (%s) disconnected. Total clients: %d", client.ID, client.Conn.RemoteAddr(), len(rs.clients))

				// Notify all remaining clients about the disconnected client
				for _, existingClient := range rs.clients {
					message := make([]byte, 1+4) // MessageType + ID
					message[0] = MsgTypeClientDisconnected
					binary.BigEndian.PutUint32(message[1:], uint32(client.ID))
					go func(c net.Conn, msg []byte) {
						_, err := c.Write(msg)
						if err != nil {
							log.Printf("Error sending client disconnected notification to %s: %v", c.RemoteAddr(), err)
						}
					}(existingClient.Conn, message)
				}
			}
			rs.mu.Unlock()

		case messageWithHeader := <-rs.broadcast:
			rs.mu.Lock()
			// The first 5 bytes of messageWithHeader are: 1 byte MsgTypeFishNetData + 4 bytes SenderID
			// We need to extract the sender ID to avoid sending back to the sender
			if len(messageWithHeader) >= 5 && messageWithHeader[0] == MsgTypeFishNetData {
				senderID := int(binary.BigEndian.Uint32(messageWithHeader[1:5]))
				for _, client := range rs.clients {
					// Broadcast to all *other* clients
					if client.ID != senderID {
						go func(c net.Conn, msg []byte) {
							_, err := c.Write(msg)
							if err != nil {
								log.Printf("Error sending broadcast message to client %s: %v", c.RemoteAddr(), err)
								// Consider unregistering client if send fails repeatedly
							}
						}(client.Conn, messageWithHeader)
					}
				}
			} else {
				log.Printf("Received invalid broadcast message: %x", messageWithHeader)
			}
			rs.mu.Unlock()
		}
	}
}

// handleClient reads data from a client, prepends sender ID, and sends it to the broadcast channel
func (rs *RelayServer) handleClient(client *Client) {
	defer func() {
		rs.unregister <- client // Ensure client is unregistered on exit
	}()

	// Buffer to read incoming data. We need space for the header when sending it out.
	// The client sends raw FishNet data. We'll add the header *before* broadcasting.
	// So, we read into a buffer, then create a new buffer with header for broadcast.
	readBuffer := make([]byte, BUFFER_SIZE-5) // Make space for the 5-byte header when broadcasting

	for {
		client.Conn.SetReadDeadline(time.Now().Add(5 * time.Minute))

		n, err := client.Conn.Read(readBuffer) // Read client's raw data
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				log.Printf("Client %d (%s) timed out.", client.ID, client.Conn.RemoteAddr())
			} else {
				log.Printf("Error reading from client %d (%s): %v", client.ID, client.Conn.RemoteAddr(), err)
			}
			break
		}

		if n > 0 {
			log.Printf("Received %d bytes from client %d (%s) for relaying.", n, client.ID, client.Conn.RemoteAddr())

			// Prepend our custom header: MessageType (1 byte) + SenderID (4 bytes)
			messageWithHeader := make([]byte, 5+n)
			messageWithHeader[0] = MsgTypeFishNetData                            // Set message type
			binary.BigEndian.PutUint32(messageWithHeader[1:], uint32(client.ID)) // Set sender ID
			copy(messageWithHeader[5:], readBuffer[:n])                          // Copy actual FishNet data

			rs.broadcast <- messageWithHeader
		}
	}
}

func main() {
	server := NewRelayServer()
	server.Run()
}
