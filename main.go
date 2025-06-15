package main

import (
	"encoding/binary"
	"log"
	"net"
	"sync"
	"time"
)

const (
	PORT = ":7777" // Port for the relay server to listen on

	// Max size of a single read from TCP stream.
	// This should be larger than any single message, considering header + payload.
	MAX_READ_BUFFER_SIZE = 8192

	// Header structure:
	// [1 byte: MessageType]
	// [4 bytes: SenderID]
	// [1 byte: RoomIDLength]
	// [Variable: RoomID (string, max 255 chars)]
	// [2 bytes: FishNetPayloadLength (ushort, for MsgTypeFishNetData)]
	// [Variable: FishNetPayload]

	// Minimum header size for control messages (MessageType + SenderID + RoomIDLength)
	MIN_HEADER_SIZE = 1 + 4 + 1 // 6 bytes
	// Additional size for FishNet payload length
	FISHNET_PAYLOAD_LENGTH_SIZE = 2
)

// Message Types for internal relay communication
const (
	MsgTypeFishNetData        byte = 0 // Regular FishNet data
	MsgTypeNewClientConnected byte = 1 // Notification that a new client connected to the relay
	MsgTypeClientDisconnected byte = 2 // Notification that a client disconnected from the relay
	MsgTypeCreateRoom         byte = 3 // Client requests to create a room
	MsgTypeJoinRoom           byte = 4 // Client requests to join a room
	MsgTypeRoomCreated        byte = 5 // Relay confirms room creation (sent to host)
	MsgTypeRoomJoined         byte = 6 // Relay confirms room joined (sent to client)
	MsgTypeRoomError          byte = 7 // Relay sends room error (e.g., room full, not found)
)

// Room represents a game room
type Room struct {
	ID        string
	HostID    int
	ClientIDs map[int]bool // Set of client IDs in this room
}

// Client represents a connected Unity client
type Client struct {
	ID       int // Unique ID assigned by the relay server
	Conn     net.Conn
	RoomID   string      // The ID of the room the client is currently in
	sendChan chan []byte // Channel to send messages specifically to this client
}

// RelayServer holds the state of the server
type RelayServer struct {
	clients      map[int]*Client
	rooms        map[string]*Room // Map of RoomID to Room
	nextClientID int
	mu           sync.Mutex // Mutex to protect clients and rooms maps
	register     chan *Client
	unregister   chan *Client
	processMsg   chan ClientMessage // Channel to process incoming messages from clients
}

// ClientMessage wraps an incoming message with the sender's client ID
type ClientMessage struct {
	SenderID    int
	MessageType byte
	RoomID      string
	Payload     []byte // The actual FishNet data or other relevant payload
}

// NewRelayServer creates and returns a new RelayServer instance
func NewRelayServer() *RelayServer {
	return &RelayServer{
		clients:      make(map[int]*Client),
		rooms:        make(map[string]*Room),
		nextClientID: 1, // Start client IDs from 1
		register:     make(chan *Client),
		unregister:   make(chan *Client),
		processMsg:   make(chan ClientMessage),
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

	go rs.manageStateAndMessages() // Start goroutine to manage client/room state and process messages

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		rs.mu.Lock()
		client := &Client{
			ID:       rs.nextClientID,
			Conn:     conn,
			sendChan: make(chan []byte, 100), // Buffered channel for sending to this client
		}
		rs.nextClientID++
		rs.mu.Unlock()

		rs.register <- client           // Register the new client
		go rs.handleClientRead(client)  // Goroutine for reading from client
		go rs.handleClientWrite(client) // Goroutine for writing to client
	}
}

// handleClientRead reads data from a client and sends it to the processMsg channel
func (rs *RelayServer) handleClientRead(client *Client) {
	defer func() {
		rs.unregister <- client // Ensure client is unregistered on exit
	}()

	buffer := make([]byte, MAX_READ_BUFFER_SIZE)
	messageBuffer := []byte{} // Buffer to accumulate partial messages

	for {
		client.Conn.SetReadDeadline(time.Now().Add(5 * time.Minute))

		n, err := client.Conn.Read(buffer)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				log.Printf("Client %d (%s) timed out.", client.ID, client.Conn.RemoteAddr())
			} else {
				log.Printf("Error reading from client %d (%s): %v", client.ID, client.Conn.RemoteAddr(), err)
			}
			break
		}

		if n > 0 {
			messageBuffer = append(messageBuffer, buffer[:n]...)

			for len(messageBuffer) >= MIN_HEADER_SIZE {
				messageType := messageBuffer[0]
				senderID := int(binary.BigEndian.Uint32(messageBuffer[1:5]))
				roomIDLength := int(messageBuffer[5])

				// Check if we have enough data for the room ID
				expectedHeaderAndRoomIDSize := MIN_HEADER_SIZE + roomIDLength
				if len(messageBuffer) < expectedHeaderAndRoomIDSize {
					break // Not enough data for full header and room ID, wait for more
				}

				roomIDBytes := messageBuffer[MIN_HEADER_SIZE:expectedHeaderAndRoomIDSize]
				roomID := string(roomIDBytes)

				// Determine the full message length based on message type
				messageLength := expectedHeaderAndRoomIDSize
				payload := []byte{}

				if messageType == MsgTypeFishNetData {
					if len(messageBuffer) < expectedHeaderAndRoomIDSize+FISHNET_PAYLOAD_LENGTH_SIZE {
						break // Not enough data for FishNet payload length
					}
					fishNetPayloadLength := int(binary.BigEndian.Uint16(messageBuffer[expectedHeaderAndRoomIDSize : expectedHeaderAndRoomIDSize+FISHNET_PAYLOAD_LENGTH_SIZE]))
					messageLength += FISHNET_PAYLOAD_LENGTH_SIZE + fishNetPayloadLength

					if len(messageBuffer) < messageLength {
						break // Not enough data for full FishNet payload
					}
					payload = messageBuffer[expectedHeaderAndRoomIDSize+FISHNET_PAYLOAD_LENGTH_SIZE : messageLength]

				} else {
					// For control messages, the payload is typically just the header
					// and potentially a small additional payload if defined.
					// For simplicity, assuming no additional payload beyond header for control messages
					// unless explicitly defined.
				}

				if len(messageBuffer) < messageLength {
					// This should ideally not happen if above checks are correct, but as a safeguard.
					break
				}

				// Process the message
				msg := ClientMessage{
					SenderID:    senderID,
					MessageType: messageType,
					RoomID:      roomID,
					Payload:     payload,
				}
				rs.processMsg <- msg

				// Consume the processed message from the buffer
				messageBuffer = messageBuffer[messageLength:]
			}
		}
	}
}

// handleClientWrite continuously writes messages from the client's sendChan to its connection
func (rs *RelayServer) handleClientWrite(client *Client) {
	for msg := range client.sendChan {
		_, err := client.Conn.Write(msg)
		if err != nil {
			log.Printf("Error sending message to client %d (%s): %v", client.ID, client.Conn.RemoteAddr(), err)
			return // Exit goroutine if write fails
		}
	}
}

// manageStateAndMessages manages client and room state, and processes all incoming messages
func (rs *RelayServer) manageStateAndMessages() {
	for {
		select {
		case client := <-rs.register:
			rs.mu.Lock()
			rs.clients[client.ID] = client
			log.Printf("Client %d connected from %s. Total clients: %d", client.ID, client.Conn.RemoteAddr(), len(rs.clients))
			rs.mu.Unlock()

			// Send new client its own ID as a NewClientConnected message (empty room ID)
			// This is essential for the client to know its Relay ID
			rs.sendToClient(client.ID, MsgTypeNewClientConnected, "", client.ID, nil)

		case client := <-rs.unregister:
			rs.mu.Lock()
			if _, ok := rs.clients[client.ID]; ok {
				// If client was in a room, remove them
				if client.RoomID != "" {
					if room, exists := rs.rooms[client.RoomID]; exists {
						delete(room.ClientIDs, client.ID)
						log.Printf("Client %d left room %s. Clients in room: %d", client.ID, client.RoomID, len(room.ClientIDs))
						// If host leaves, disband room (or transfer host)
						if room.HostID == client.ID {
							log.Printf("Host %d for room %s disconnected. Disbanding room.", client.ID, client.RoomID)
							rs.disbandRoom(client.RoomID)
						} else if len(room.ClientIDs) == 0 {
							// If last client leaves, remove empty room
							log.Printf("Room %s is now empty. Removing room.", client.RoomID)
							delete(rs.rooms, client.RoomID)
						}
						// Notify remaining clients in the room about disconnection
						rs.broadcastInRoom(client.RoomID, MsgTypeClientDisconnected, client.RoomID, client.ID, nil, client.ID) // Don't send to self
					}
				}
				delete(rs.clients, client.ID)
				close(client.sendChan) // Close the send channel
				client.Conn.Close()
				log.Printf("Client %d (%s) disconnected. Total clients: %d", client.ID, client.Conn.RemoteAddr(), len(rs.clients))
			}
			rs.mu.Unlock()

		case msg := <-rs.processMsg:
			rs.mu.Lock()
			senderClient, clientExists := rs.clients[msg.SenderID]
			if !clientExists {
				log.Printf("Received message from unknown client ID %d. Discarding.", msg.SenderID)
				rs.mu.Unlock()
				continue
			}

			switch msg.MessageType {
			case MsgTypeCreateRoom:
				if senderClient.RoomID != "" {
					rs.sendToClient(msg.SenderID, MsgTypeRoomError, msg.RoomID, msg.SenderID, []byte("Already in a room."))
					log.Printf("Client %d tried to create room %s but is already in room %s", msg.SenderID, msg.RoomID, senderClient.RoomID)
					break
				}
				if _, exists := rs.rooms[msg.RoomID]; exists {
					rs.sendToClient(msg.SenderID, MsgTypeRoomError, msg.RoomID, msg.SenderID, []byte("Room already exists."))
					log.Printf("Client %d tried to create room %s, but it already exists.", msg.SenderID, msg.RoomID)
				} else {
					room := &Room{
						ID:        msg.RoomID,
						HostID:    msg.SenderID,
						ClientIDs: make(map[int]bool),
					}
					room.ClientIDs[msg.SenderID] = true // Add host to the room
					rs.rooms[msg.RoomID] = room
					senderClient.RoomID = msg.RoomID                                                 // Assign client to room
					rs.sendToClient(msg.SenderID, MsgTypeRoomCreated, msg.RoomID, msg.SenderID, nil) // Confirm creation
					log.Printf("Client %d created room %s. Host: %d", msg.SenderID, msg.RoomID, msg.SenderID)
				}

			case MsgTypeJoinRoom:
				if senderClient.RoomID != "" {
					rs.sendToClient(msg.SenderID, MsgTypeRoomError, msg.RoomID, msg.SenderID, []byte("Already in a room."))
					log.Printf("Client %d tried to join room %s but is already in room %s", msg.SenderID, msg.RoomID, senderClient.RoomID)
					break
				}
				if room, exists := rs.rooms[msg.RoomID]; exists {
					room.ClientIDs[msg.SenderID] = true                                             // Add client to the room
					senderClient.RoomID = msg.RoomID                                                // Assign client to room
					rs.sendToClient(msg.SenderID, MsgTypeRoomJoined, msg.RoomID, msg.SenderID, nil) // Confirm join

					log.Printf("Client %d joined room %s. Clients in room: %d", msg.SenderID, msg.RoomID, len(room.ClientIDs))
					// Notify everyone in the room about the new client (including host)
					rs.broadcastInRoom(msg.RoomID, MsgTypeNewClientConnected, msg.RoomID, msg.SenderID, nil, 0) // Broadcast to all in room
				} else {
					rs.sendToClient(msg.SenderID, MsgTypeRoomError, msg.RoomID, msg.SenderID, []byte("Room not found."))
					log.Printf("Client %d tried to join non-existent room %s", msg.SenderID, msg.RoomID)
				}

			case MsgTypeFishNetData:
				// Ensure client is in a room to send game data
				if senderClient.RoomID == "" {
					log.Printf("Client %d tried to send FishNet data but is not in a room. Discarding.", msg.SenderID)
					break
				}
				// Broadcast the FishNet data only within the sender's room
				rs.broadcastInRoom(senderClient.RoomID, msg.MessageType, msg.RoomID, msg.SenderID, msg.Payload, msg.SenderID) // Exclude sender

			default:
				log.Printf("Received unknown message type %d from client %d", msg.MessageType, msg.SenderID)
			}
			rs.mu.Unlock()
		}
	}
}

// sendToClient prepares and sends a message to a specific client
func (rs *RelayServer) sendToClient(targetClientID int, msgType byte, roomID string, senderID int, payload []byte) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	client, exists := rs.clients[targetClientID]
	if !exists {
		log.Printf("Attempted to send message to non-existent client %d", targetClientID)
		return
	}

	// Calculate total message length
	roomIDBytes := []byte(roomID)
	roomIDLength := len(roomIDBytes)
	if roomIDLength > 255 {
		log.Printf("Room ID too long for client %d", targetClientID)
		return
	}

	payloadLength := 0
	if payload != nil {
		payloadLength = len(payload)
	}

	messageSize := MIN_HEADER_SIZE + roomIDLength
	if msgType == MsgTypeFishNetData {
		messageSize += FISHNET_PAYLOAD_LENGTH_SIZE + payloadLength
	} else if payload != nil {
		// For control messages with optional payload (e.g., error messages)
		messageSize += payloadLength // Assume payload length is not prefixed for control payloads
	}

	fullMessage := make([]byte, messageSize)
	offset := 0

	fullMessage[offset] = msgType
	offset++

	binary.BigEndian.PutUint32(fullMessage[offset:], uint32(senderID))
	offset += 4

	fullMessage[offset] = byte(roomIDLength)
	offset++

	copy(fullMessage[offset:], roomIDBytes)
	offset += roomIDLength

	if msgType == MsgTypeFishNetData {
		binary.BigEndian.PutUint16(fullMessage[offset:], uint16(payloadLength))
		offset += 2
	}
	if payload != nil {
		copy(fullMessage[offset:], payload)
	}

	select {
	case client.sendChan <- fullMessage:
		// Message sent to client's send channel
	default:
		log.Printf("Client %d send channel full, dropping message.", targetClientID)
	}
}

// broadcastInRoom sends a message to all clients in a specific room, excluding an optional client
func (rs *RelayServer) broadcastInRoom(roomID string, msgType byte, roomIDForHeader string, senderID int, payload []byte, excludeClientID int) {
	room, exists := rs.rooms[roomID]
	if !exists {
		log.Printf("Attempted to broadcast in non-existent room %s", roomID)
		return
	}

	for clientInRoomID := range room.ClientIDs {
		if clientInRoomID == excludeClientID {
			continue // Don't send to the excluded client (e.g., sender)
		}
		rs.sendToClient(clientInRoomID, msgType, roomIDForHeader, senderID, payload)
	}
}

// disbandRoom removes a room and notifies its clients
func (rs *RelayServer) disbandRoom(roomID string) {
	room, exists := rs.rooms[roomID]
	if !exists {
		return
	}

	for clientID := range room.ClientIDs {
		if client, clientExists := rs.clients[clientID]; clientExists {
			client.RoomID = "" // Remove client from room
			rs.sendToClient(clientID, MsgTypeRoomError, roomID, room.HostID, []byte("Room disbanded by host."))
		}
	}
	delete(rs.rooms, roomID)
	log.Printf("Room %s has been disbanded.", roomID)
}

func main() {
	server := NewRelayServer()
	server.Run()
}
