package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const FOLLOWER_STATE = "follower"
const CANDIDATE_STATE = "candidate"
const LEADER_STATE = "leader"

var serverPorts = []int{8080, 8081, 8082, 8083, 8084}
var quorum = len(serverPorts)/2 + 1

type config struct {
	port    int
	timeout int
}

type server struct {
	state    string
	term     int
	isVoted  bool
	commChan chan commResult
	config   config
	mu       sync.Mutex
}

func main() {
	var config config
	flag.IntVar(&config.port, "port", 8080, "Server Port")
	flag.IntVar(&config.timeout, "timeout", 3000, "Election timeout")
	flag.Parse()

	log.Println(config.timeout)
	log.Println(config.port)

	commChan := make(chan commResult)

	server := server{
		state:    FOLLOWER_STATE,
		term:     0,
		isVoted:  false,
		commChan: commChan,
		config:   config,
	}

	go server.timeoutLoop(commChan)
	server.serve()
}

func (s *server) timeoutLoop(commChan chan commResult) {
	ticker := time.NewTicker(time.Millisecond * time.Duration(s.config.timeout+rand.Intn(s.config.timeout)))
	for {
		select {
		case status := <-commChan:
			if status.cmdType == "heartbeat" {
				if status.successful {
					s.resetStateToFollower()
					ticker.Reset(time.Millisecond * time.Duration(s.config.timeout+rand.Intn(s.config.timeout)))
				}
			} else if status.cmdType == "election" {
				if status.successful {
					break
				} else {
					s.resetStateToFollower()
					ticker.Reset(time.Millisecond * time.Duration(s.config.timeout+rand.Intn(s.config.timeout)))
				}
			}
		case <-ticker.C:
			go startElection(s)
		}
	}
}

func (s *server) resetStateToFollower() {
	log.Printf("Reset to Follower state on server: %d\n", s.config.port)
	s.mu.Lock()
	s.isVoted = false
	s.state = FOLLOWER_STATE
	s.mu.Unlock()
}

func (s *server) promoteToLeader() {
	s.mu.Lock()

	if s.state == FOLLOWER_STATE || s.state == LEADER_STATE {
		return
	}

	s.isVoted = false
	s.state = LEADER_STATE
	s.mu.Unlock()
	log.Printf("Leader promoted on server: %d\n", s.config.port)
	s.handleLeaderTask()

}

type getResult struct {
	successful bool
	err        error
}

type commResult struct {
	successful bool
	term       int
	cmdType    string
}

func startElection(server *server) {
	fmt.Println("Start self voting")
	server.mu.Lock()
	server.term += 1
	server.state = CANDIDATE_STATE
	server.isVoted = true
	currentTerm := server.term
	server.mu.Unlock()
	log.Println("break point")
	go consultOtherServer(server, currentTerm)
}

func consultOtherServer(server *server, currentTerm int) {
	fmt.Println("Start consult other servers")
	ch := make(chan getResult)

	log.Println(123)

	for _, port := range serverPorts {
		if port != server.config.port {
			go get(fmt.Sprintf("http://localhost:%d/vote?term=%d", port, server.term), ch)
		}
	}

	var count int = 1
	for _, port := range serverPorts {
		if port == server.config.port {
			continue
		}
		result := <-ch
		log.Println(result.successful)
		if result.successful {
			count++
			log.Printf("count: %d\n", count)
			if count >= quorum {
				server.commChan <- commResult{successful: true, term: currentTerm, cmdType: "election"}
				return
			}
		}
	}

	// log.Println(123)
	server.commChan <- commResult{successful: false, term: currentTerm, cmdType: "election"}

}

func get(url string, ch chan<- getResult) {
	var result getResult

	if resp, err := http.Get(url); err != nil {
		result = getResult{false, err}
	} else {
		bytes, err := io.ReadAll(resp.Body)
		if err != nil {
			result = getResult{false, err}
		} else {
			successful, err := strconv.ParseBool(string(bytes))
			if err != nil {
				result = getResult{false, err}
			} else {
				result = getResult{successful, nil}
			}
		}
		resp.Body.Close()
	}

	if ch != nil {
		ch <- result
	}
}

func (s *server) handleLeaderTask() {
	ticker := time.NewTicker(time.Millisecond * time.Duration(s.config.timeout))
	immediateTicker := time.NewTicker(time.Duration(1))
	for {
		select {
		case <-immediateTicker.C:
			s.sendHeartbeat()
			immediateTicker.Stop()
		case <-ticker.C:
			s.sendHeartbeat()
		}
	}
}

func (s *server) sendHeartbeat() {
	log.Printf("Leader start sending heartbeat server: %d\n", s.config.port)
	for _, port := range serverPorts {
		if port != s.config.port {
			go get(fmt.Sprintf("http://localhost:%d/heartbeat?term=%d", port, s.term), nil)
		}
	}
}

func (s *server) serve() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/vote", s.voteHandle)
	mux.HandleFunc("/heartbeat", s.heartbeatHandle)
	return http.ListenAndServe(fmt.Sprintf(":%d", s.config.port), mux)
}

func (s *server) voteHandle(rw http.ResponseWriter, r *http.Request) {
	str := r.URL.Query().Get("term")
	requestTerm, err := strconv.Atoi(str)
	if err != nil {
		log.Fatalln("could not find term")
	}
	result := "false"

	s.mu.Lock()
	if !s.isVoted && s.term <= requestTerm {
		result = "true"
		s.promoteToLeader()
	}
	s.mu.Unlock()

	rw.Write([]byte(result))

}

func (s *server) heartbeatHandle(rw http.ResponseWriter, r *http.Request) {
	str := r.URL.Query().Get("term")

	requestTerm, err := strconv.Atoi(str)
	if err != nil {
		log.Fatalln("could not find term")
	}

	s.mu.Lock()
	if s.term <= requestTerm {
		s.term = requestTerm
		s.commChan <- commResult{successful: true, term: requestTerm, cmdType: "heartbeat"}
	}
	s.mu.Unlock()

}
