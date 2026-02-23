package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/gorilla/websocket"
)

type PlayerState struct {
	Board  [3][3]int `json:"board"`
	Pieces [9]bool   `json:"pieces"`
}

type GameState struct {
	Players      [2]PlayerState `json:"players"`
	CurrentImage int            `json:"currentImage"`
}

type Move struct {
	Player int `json:"player"`
	X      int `json:"x"`
	Y      int `json:"y"`
	Piece  int `json:"piece"`
}

type IncomingMessage struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type OutgoingMessage struct {
	Type  string     `json:"type"`
	Data  *GameState `json:"data,omitempty"`
	Piece int        `json:"piece,omitempty"`
}

var (
	state   GameState
	mu      sync.Mutex
	clients = make(map[*websocket.Conn]bool)

	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
)

func initGame() {
	for p := 0; p < 2; p++ {
		for i := 0; i < 9; i++ {
			state.Players[p].Pieces[i] = true
			state.Players[p].Board[i/3][i%3] = 0
		}
	}
	state.CurrentImage = 0
}

// broadcast sends the current game state to all connected clients
func broadcast() {
	mu.Lock()
	current := state
	mu.Unlock()

	for c := range clients {
		c.WriteJSON(OutgoingMessage{
			Type: "state",
			Data: &current,
		})
	}
}

// checkComplete returns true if all cells in a player's board are filled
func checkComplete(player PlayerState) bool {
	for y := 0; y < 3; y++ {
		for x := 0; x < 3; x++ {
			if player.Board[y][x] == 0 {
				return false
			}
		}
	}
	return true
}

// resetBoards resets both players' boards and pieces, then cycles the image
func resetBoards() {
	for p := 0; p < 2; p++ {
		for i := 0; i < 9; i++ {
			state.Players[p].Pieces[i] = true
			state.Players[p].Board[i/3][i%3] = 0
		}
	}
	state.CurrentImage = (state.CurrentImage + 1) % 3
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	mu.Lock()
	clients[conn] = true
	current := state
	mu.Unlock()

	// send initial state
	conn.WriteJSON(OutgoingMessage{
		Type: "state",
		Data: &current,
	})

	defer func() {
		mu.Lock()
		delete(clients, conn)
		mu.Unlock()
		conn.Close()
	}()

	for {
		var msg IncomingMessage
		if err := conn.ReadJSON(&msg); err != nil {
			break
		}

		if msg.Type == "move" {
			var move Move
			if err := json.Unmarshal(msg.Data, &move); err != nil {
				continue
			}

			mu.Lock()
			valid := false

			if move.Player >= 0 && move.Player < 2 &&
				move.X >= 0 && move.X < 3 &&
				move.Y >= 0 && move.Y < 3 &&
				move.Piece >= 0 && move.Piece < 9 {

				expected := move.Y*3 + move.X

				if move.Piece == expected &&
					state.Players[move.Player].Pieces[move.Piece] &&
					state.Players[move.Player].Board[move.Y][move.X] == 0 {

					state.Players[move.Player].Board[move.Y][move.X] = move.Piece + 1
					state.Players[move.Player].Pieces[move.Piece] = false
					valid = true
				}
			}

			// If this move completes the puzzle, reset boards and advance image
			if valid && checkComplete(state.Players[move.Player]) {
				resetBoards()
			}

			mu.Unlock()

			if valid {
				broadcast()
			} else {
				conn.WriteJSON(OutgoingMessage{
					Type:  "invalidMove",
					Piece: move.Piece,
				})
			}
		}
	}
}

func main() {
	initGame()

	http.Handle("/", http.FileServer(http.Dir("./public")))
	http.HandleFunc("/ws", wsHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Println("Server running on port", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
