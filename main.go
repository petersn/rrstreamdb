package main

import (
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	_ "github.com/lib/pq"
	"gopkg.in/yaml.v2"
	_ "gopkg.in/yaml.v2"
)

var upgrader = websocket.Upgrader{}

type ServerConfig struct {
	PostgresUrl string `yaml:"postgresUrl"`
	Host        string `yaml:"host"`
	AuthToken   string `yaml:"authToken"`
}

type ProtocolRequest struct {
	Kind  string                 `json:"kind"`
	Query map[string]interface{} `json:"query"`
}

type Query struct {
	LogName          string
	GroupByColumn    string
	MostRecentColumn string        // Either the field name to group by, or "" to mean no grouping
	FilterColumn     string        // The column to filter on, or "" to mean no filtering
	FilterValues     []interface{} // Collection of values to be interested in from the column
}

type Subscription struct {
	Queries []Query
}

type Server struct {
	Config      *ServerConfig
	Database    *sql.DB
	Mux         sync.Mutex
	Subscribers map[chan string]Subscription
	MostRecent  map[string]string
}

func (serv *Server) wsEndpoint(w http.ResponseWriter, r *http.Request) {
	upgrader.CheckOrigin = func(r *http.Request) bool { return true }
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}
	defer c.Close()

	// Read an initial auth message that must be equal to the secret password.
	mt, message, err := c.ReadMessage()
	if err != nil || mt != websocket.TextMessage {
		log.Print("bad auth message:", err)
		return
	}
	if subtle.ConstantTimeCompare(message, []byte(serv.Config.AuthToken)) == 0 {
		log.Print("bad password:", err)
		return
	}

	wakeupChannel := make(chan string)
	serv.Subscribers[wakeupChannel] = Subscription{}
	defer delete(serv.Subscribers, wakeupChannel)

	messageChannel := make(chan string)
	go func() {
		for {
			mt, message, err := c.ReadMessage()
			if err != nil || mt != websocket.TextMessage {
				log.Println("read:", err)
				break
			}
			messageChannel <- string(message)
		}
		close(messageChannel)
	}()

	for {
		select {
		case message := <-messageChannel:
			var protocolRequest ProtocolRequest
			err = json.Unmarshal([]byte(message), &protocolRequest)
			if err != nil {
				log.Println("failed to parse:", err)
				continue
			}
			switch protocolRequest.Kind {
			case "ping":
				err = c.WriteMessage(websocket.TextMessage, []byte("pong"))
				if err != nil {
					log.Println("write:", err)
					return
				}
			case "query":

			default:
				log.Printf("bad request: %#v", protocolRequest.Kind)
				continue
			}
		case <-wakeupChannel:
			//c.WriteMessage()
		}
	}
}

func main() {
	configPath := flag.String("config", "streamdb-config.yaml", "Config file to load")
	flag.Parse()

	yamlFile, err := ioutil.ReadFile(*configPath)
	if err != nil {
		panic(err)
	}
	var config ServerConfig
	err = yaml.Unmarshal(yamlFile, &config)

	// const (
	// 	postgresHost = "localhost"
	// 	port         = 5432
	// 	user         = "postgres"
	// 	password     = "ceb43a1233c55f2aed4d32c599b30bf2"
	// 	dbname       = "postgres"
	// )
	// psqlInfo := fmt.Sprintf(
	// 	"host=%s port=%d user=%s password=%s dbname=%s",
	// 	postgresHost, port, user, password, dbname,
	// )
	//psqlInfo = "postgresql://postgres:ceb43a1233c55f2aed4d32c599b30bf2@localhost:5432/postgres"
	db, err := sql.Open("postgres", config.PostgresUrl)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		panic(err)
	}

	fmt.Printf("StreamDB binding to %s\n", config.Host)

	server := Server{
		Config:      &config,
		Database:    db,
		Subscribers: make(map[chan string]Subscription),
		MostRecent:  make(map[string]string),
	}

	http.HandleFunc("/", server.wsEndpoint)
	log.Fatal(http.ListenAndServe(config.Host, nil))
}
