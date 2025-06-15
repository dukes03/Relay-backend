// relay.go
package main

import (
	"encoding/binary"
	"log"
	"net"
	"sync"
	"time"
)

const (
	// maximum UDP packet size
	MaxBufSize = 1500
)

type Client struct {
	Addr     *net.UDPAddr
	ID       uint16
	LastSeen time.Time
}

func main() {
	addr := net.UDPAddr{IP: net.IPv4zero, Port: 7777}
	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		log.Fatalf("ไม่สามารถเปิดพอร์ตได้: %v", err)
	}
	defer conn.Close()
	log.Printf("Relay server กำลังรันที่ %s\n", addr.String())

	clients := make(map[uint16]*Client)
	var mu sync.Mutex
	var nextID uint16 = 1

	buf := make([]byte, MaxBufSize)
	for {
		n, remote, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("Read error: %v\n", err)
			continue
		}
		if n < 2 {
			log.Printf("แพ็กเก็ตสั้นเกินไปจาก %s\n", remote.String())
			continue
		}

		// ดึง destID จาก 2 ไบต์แรก
		destID := binary.BigEndian.Uint16(buf[:2])
		payload := buf[2:n]

		mu.Lock()
		// ถ้า sender ยังไม่เคยลงทะเบียน ให้สร้าง ID ใหม่ให้
		var senderID uint16
		found := false
		for _, c := range clients {
			if c.Addr.IP.Equal(remote.IP) && c.Addr.Port == remote.Port {
				senderID = c.ID
				c.LastSeen = time.Now()
				found = true
				break
			}
		}
		if !found {
			senderID = nextID
			clients[senderID] = &Client{Addr: remote, ID: senderID, LastSeen: time.Now()}
			nextID++
			log.Printf("[Connect] Client ใหม่: ID=%d, Addr=%s\n", senderID, remote.String())
		}

		// ถ้า destID = 0 ให้ broadcast ไปทุกคน (เช่น handshake / discover)
		if destID == 0 {
			for _, c := range clients {
				if c.ID != senderID {
					// ใส่ header กลับเป็น senderID เพื่อให้ปลายทางรู้ว่าใครส่งมา
					hdr := make([]byte, 2)
					binary.BigEndian.PutUint16(hdr, senderID)
					conn.WriteToUDP(append(hdr, payload...), c.Addr)
				}
			}
			log.Printf("[Broadcast] จาก %d ไป %d client\n", senderID, len(clients)-1)
		} else {
			// ส่งเฉพาะไป destID
			if c, ok := clients[destID]; ok {
				hdr := make([]byte, 2)
				binary.BigEndian.PutUint16(hdr, senderID)
				conn.WriteToUDP(append(hdr, payload...), c.Addr)
				log.Printf("[Relay] %d → %d, %d ไบต์\n", senderID, destID, len(payload))
			} else {
				log.Printf("[Warn] ไม่พบ destID=%d จาก client %d\n", destID, senderID)
			}
		}
		mu.Unlock()

		// ลบ client ที่หมดเวลาส่ง data เกิน 60 วินาที
		go func() {
			mu.Lock()
			defer mu.Unlock()
			now := time.Now()
			for id, c := range clients {
				if now.Sub(c.LastSeen) > 60*time.Second {
					log.Printf("[Timeout] Client ID=%d ล้มเหลวเนื่องจาก timeout\n", id)
					delete(clients, id)
				}
			}
		}()
	}
}
