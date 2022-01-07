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
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	_ "github.com/lib/pq"
	"gopkg.in/yaml.v2"
)

var sqlTypeMapping = map[string]string{
	"Text":            "TEXT NOT NULL",
	"Float":           "DOUBLE PRECISION NOT NULL",
	"Integer":         "INTEGER NOT NULL",
	"Boolean":         "BOOLEAN NOT NULL",
	"NullableText":    "TEXT",
	"NullableFloat":   "DOUBLE PRECISION",
	"NullableInteger": "INTEGER",
	"NullableBoolean": "BOOLEAN",
	"JSON":            "JSONB NOT NULL",
}

var upgrader = websocket.Upgrader{}

type DataRows struct {
	Data         map[string][]interface{}
	Length       int
	MostRecentId int64
}

func (dr *DataRows) Recompute() {
	dr.Length = 0
	for _, slice := range dr.Data {
		dr.Length = len(slice)
		break
	}
	dr.MostRecentId = -1
	if ids, ok := dr.Data["id"]; ok {
		dr.MostRecentId = ids[len(ids)-1].(int64)
	}
}

type ServerConfig struct {
	PostgresUrl    string `yaml:"postgresUrl"`
	Host           string `yaml:"host"`
	ReadToken      string `yaml:"readToken"`
	ReadWriteToken string `yaml:"readWriteToken"`
	AdminToken     string `yaml:"adminToken"`
}

type SubscriptionSpec struct {
	TableName     string `yaml:"table"`
	GroupByColumn string `yaml:"groupBy"` // "" means no grouping
	MostRecent    bool   `yaml:"mostRecent"`
}

type ServerSchema struct {
	Tables map[string]struct {
		Fields map[string]interface{} `yaml:"fields"`
	} `yaml:"tables"`
	Subscriptions map[string]SubscriptionSpec `yaml:"subscriptions"`
}

type ProtocolRequest struct {
	Kind          string                 `json:"kind"`
	Token         int64                  `json:"token"`
	Subscription  string                 `json:"subscription"`
	Table         string                 `json:"table"`
	Cursor        int64                  `json:"cursor"`
	FilterCursors [][]interface{}        `json:"filterCursors"`
	Limit         int                    `json:"limit"`
	Row           map[string]interface{} `json:"row"`
	Rows          DataRows               `json:"rows"`
}

type SubscriptionCursor struct {
	SubscriptionName string
	Cursor           int64
	Limit            int
	// Collection of values to be interested in from the column, and the newest MostRecentId we know of for each
	FilterCursors map[interface{}]*int64
}

type SubscriptionState struct {
	SubscriptionSpec SubscriptionSpec
	GroupByRows      map[interface{}]*DataRows
	RegularRows      DataRows
}

type ClientState struct {
	WritePermissionBit bool
	AdminPermissionBit bool
	Cursors            map[int64]*SubscriptionCursor
}

type Server struct {
	Config        *ServerConfig
	Schema        *ServerSchema
	Database      *sql.DB
	Mux           sync.Mutex
	Clients       map[chan string]*ClientState
	Subscriptions map[string]*SubscriptionState
}

func (serv *Server) RefreshSubscription(subName string) error {
	//fmt.Printf("\x1b[91mRefreshing subscription\x1b[0m: %s\n", subName)
	subSpec := serv.Schema.Subscriptions[subName]
	subState := SubscriptionState{SubscriptionSpec: subSpec}
	if subSpec.GroupByColumn == "" {
		subState.RegularRows = DataRows{
			Length: 0,
			Data:   make(map[string][]interface{}),
		}
	} else {
		subState.GroupByRows = make(map[interface{}]*DataRows)
	}

	var query string
	switch true {
	// Select all rows
	case subSpec.GroupByColumn == "" && !subSpec.MostRecent:
		query = fmt.Sprintf("SELECT * FROM %s ORDER BY id", subSpec.TableName)

	// Select the most recent row
	case subSpec.GroupByColumn == "" && subSpec.MostRecent:
		query = fmt.Sprintf("SELECT * FROM %s ORDER BY id DESC LIMIT 1", subSpec.TableName)

	// Select all rows
	case subSpec.GroupByColumn != "" && !subSpec.MostRecent:
		query = fmt.Sprintf("SELECT * FROM %s ORDER BY id", subSpec.TableName)

	// Select just the most recent row of each group
	case subSpec.GroupByColumn != "" && subSpec.MostRecent:
		query = fmt.Sprintf(
			"SELECT * FROM %s WHERE id IN (SELECT MAX(id) FROM %s GROUP BY %s)",
			subSpec.TableName, subSpec.TableName, subSpec.GroupByColumn,
		)
	}

	// Read all of the corresponding rows
	rows, err := serv.Database.Query(query)
	if err != nil {
		log.Fatalf("could not execute query: %#v", err)
		return err
	}
	columns, err := rows.Columns()
	if err != nil {
		log.Fatalf("could not get columns: %#v", err)
		return err
	}

	groupByColumnIndex := -1
	if subSpec.GroupByColumn != "" {
		for i, columnName := range columns {
			if columnName == subSpec.GroupByColumn {
				groupByColumnIndex = i
			}
		}
		if groupByColumnIndex == -1 {
			return fmt.Errorf("Bad group by")
		}
	}

	for rows.Next() {
		dump := make([]interface{}, len(columns))
		dumpPtrs := make([]interface{}, len(columns))
		for i := range dump {
			dumpPtrs[i] = &dump[i]
		}
		err = rows.Scan(dumpPtrs...)
		if err != nil {
			log.Fatalf("could not scan query: %#v", err)
			return err
		}
		// Populate our stash of rows
		if subSpec.GroupByColumn == "" {
			for i, value := range dump {
				subState.RegularRows.Data[columns[i]] = append(subState.RegularRows.Data[columns[i]], value)
			}
			subState.RegularRows.Recompute()
		} else {
			groupByValue := dump[groupByColumnIndex]
			if _, ok := subState.GroupByRows[groupByValue]; !ok {
				subState.GroupByRows[groupByValue] = &DataRows{
					Length: 0,
					Data:   make(map[string][]interface{}),
				}
			}
			ourGroup := subState.GroupByRows[groupByValue]
			for i, value := range dump {
				ourGroup.Data[columns[i]] = append(ourGroup.Data[columns[i]], value)
			}
			ourGroup.Recompute()
		}
	}

	//fmt.Printf("%#v\n", subState)

	serv.Subscriptions[subName] = &subState
	return nil
}

func binarySearchForNewRecords(largestSeen int64, rowData *DataRows) int {
	// Fast path is that there's no new data.
	if largestSeen >= rowData.MostRecentId {
		return rowData.Length
	}
	//lo, hi := 0, rowData.Length-1
	// TODO: Implement binary search
	ids := rowData.Data["id"]
	for i, value := range ids {
		if value.(int64) > largestSeen {
			return i
		}
	}
	return len(ids)
}

func (serv *Server) CatchUpCursor(conn *websocket.Conn, token int64, cursor *SubscriptionCursor) error {
	//fmt.Printf("\x1b[91mCatch up cursor\x1b[0m: %v\n", cursor)
	retrievedData := make(map[string][]interface{})
	addRows := func(largestSeen int64, rowData *DataRows) {
		newIndex := binarySearchForNewRecords(largestSeen, rowData)
		//fmt.Printf("  \x1b[91mAdd rows\x1b[0m: largestSeen=%v newIndex=%v length=%v\n", largestSeen, newIndex, rowData.Length)
		if newIndex < rowData.Length {
			for fieldName, values := range rowData.Data {
				retrievedData[fieldName] = append(retrievedData[fieldName], values[newIndex:]...)
			}
		}
	}

	// Lookup the subscription state
	subState := serv.Subscriptions[cursor.SubscriptionName]
	//pp.Print(serv.Subscriptions)
	//fmt.Printf("Subscriptions: %#v\n", serv.Subscriptions)
	//fmt.Printf("Got here: %v %#v\n%#v\n", token, cursor, subState)
	if subState.SubscriptionSpec.GroupByColumn == "" {
		addRows(cursor.Cursor, &subState.RegularRows)
		cursor.Cursor = subState.RegularRows.MostRecentId
	} else {
		for cursorFilterValue, largestSeen := range cursor.FilterCursors {
			//fmt.Printf("Got here: %v %#v\n%#v\n", token, cursor, subState)
			ourGroup, ok := subState.GroupByRows[cursorFilterValue]
			if !ok {
				//pp.Print("Bad results:", subState.GroupByRows, cursorFilterValue)
				continue
			}
			addRows(*largestSeen, ourGroup)
			*cursor.FilterCursors[cursorFilterValue] = subState.RegularRows.MostRecentId
		}
	}

	// If token is -1 then we're answering a query and respond with even empty queries
	if token == -1 || len(retrievedData) > 0 {
		bytes, err := json.Marshal(struct {
			Kind  string                   `json:"kind"`
			Token int64                    `json:"token"`
			Rows  map[string][]interface{} `json:"rows"`
		}{
			Kind:  "data",
			Token: token,
			Rows:  retrievedData,
		})
		if err != nil {
			return err
		}
		err = conn.WriteMessage(websocket.TextMessage, bytes)
		if err != nil {
			return err
		}
	}
	return nil
}

func (serv *Server) AppendRows(tableName string, rowData DataRows) error {
	tableDesc, ok := serv.Schema.Tables[tableName]
	if !ok {
		return fmt.Errorf("unknown table for append: %#v", tableName)
	}
	columnNames := make([]string, 0, len(tableDesc.Fields))
	for key := range tableDesc.Fields {
		columnNames = append(columnNames, key)
	}

	// We're immediately done if we have no rows.
	if rowData.Length == 0 {
		return nil
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "INSERT INTO %s (created_at", tableName)
	for _, columnName := range columnNames {
		fmt.Fprintf(&sb, ", %s", columnName)
	}
	fmt.Fprint(&sb, ") VALUES")

	values := make([]interface{}, 0, len(columnNames)*rowData.Length)
	for i := 0; i < rowData.Length; i++ {
		if i != 0 {
			fmt.Fprint(&sb, ", ")
		}
		fmt.Fprint(&sb, " (NOW()")
		for _, columnName := range columnNames {
			fmt.Fprintf(&sb, ", $%v", len(values)+1)
			values = append(values, rowData.Data[columnName][i])
		}
		fmt.Fprint(&sb, ")")
	}
	fmt.Fprint(&sb, " RETURNING id, created_at")
	//fmt.Println(sb.String())

	ids := make([]interface{}, 0)
	createdAts := make([]interface{}, 0)
	idTimestampRows, err := serv.Database.Query(sb.String(), values...)
	if err != nil {
		return err
	}
	for idTimestampRows.Next() {
		var id int64
		var timestamp time.Time
		err = idTimestampRows.Scan(&id, &timestamp)
		if err != nil {
			return err
		}
		ids = append(ids, id)
		createdAts = append(createdAts, timestamp)
	}
	rowData.Data["id"] = ids
	rowData.Data["created_at"] = createdAts

	return serv.UpdateSubscriptions(tableName, rowData)
}

func (serv *Server) UpdateSubscriptions(tableName string, rowData DataRows) error {
	if rowData.Length == 0 {
		return nil
	}
	// First we update all relevant subscriptions.
	for _, subState := range serv.Subscriptions {
		subSpec := subState.SubscriptionSpec
		if subSpec.TableName != tableName {
			continue
		}
		if subSpec.GroupByColumn == "" {
			for columnName, values := range rowData.Data {
				if subSpec.MostRecent {
					// If we want a single ungrouped most recent record then just immediately replace.
					subState.RegularRows.Data[columnName] = []interface{}{values[len(values)-1]}
				} else {
					// If we want all records ungrouped, then add all records.
					subState.RegularRows.Data[columnName] = append(subState.RegularRows.Data[columnName], values...)
				}
			}
			subState.RegularRows.Recompute()
		} else {
			groupByColumnSlice := rowData.Data[subSpec.GroupByColumn]
			for i := 0; i < rowData.Length; i++ {
				groupByValue := groupByColumnSlice[i]
				if _, ok := subState.GroupByRows[groupByValue]; !ok {
					subState.GroupByRows[groupByValue] = &DataRows{
						Length: 0,
						Data:   make(map[string][]interface{}),
					}
				}
				ourGroup := subState.GroupByRows[groupByValue]
				for columnName, values := range rowData.Data {
					if subSpec.MostRecent {
						// If we want a single ungrouped most recent record then just immediately replace.
						ourGroup.Data[columnName] = []interface{}{values[len(values)-1]}
					} else {
						// If we want all records ungrouped, then add all records.
						ourGroup.Data[columnName] = append(ourGroup.Data[columnName], values...)
					}
				}
				ourGroup.Recompute()
			}
		}
	}
	return nil
}

func (serv *Server) WebSocketEndpoint(w http.ResponseWriter, r *http.Request) {
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

	clientState := &ClientState{
		Cursors: make(map[int64]*SubscriptionCursor),
		//Subscriptions: make(map[int64]SubscriptionState),
	}

	if subtle.ConstantTimeCompare(message, []byte(serv.Config.AdminToken)) == 1 {
		clientState.WritePermissionBit = true
		clientState.AdminPermissionBit = true
	} else if subtle.ConstantTimeCompare(message, []byte(serv.Config.ReadWriteToken)) == 1 {
		clientState.WritePermissionBit = true
	} else if subtle.ConstantTimeCompare(message, []byte(serv.Config.ReadToken)) == 0 {
		log.Print("bad auth token")
		c.WriteMessage(websocket.TextMessage, []byte("{\"kind\": \"error\", \"message\": \"bad auth token\"}"))
		return
	}

	wakeupChannel := make(chan string)
	serv.Clients[wakeupChannel] = clientState
	defer delete(serv.Clients, wakeupChannel)

	messageChannel := make(chan string)
	go func() {
		for {
			//log.Println("loop on reading message")
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
			if message == "" {
				return
			}
			errorMessage := ""
			filterCursors := make(map[interface{}]*int64)
			var protocolRequest ProtocolRequest
			err = json.Unmarshal([]byte(message), &protocolRequest)
			if err != nil {
				log.Println("failed to parse:", err)
				errorMessage = fmt.Sprintf("failed to parse: %s", err)
				goto bad
			}

			for _, cursor := range protocolRequest.FilterCursors {
				if len(cursor) != 2 {
					errorMessage = "each filterCursor entry must be of length two, like: [\"foo\", 37]"
					goto bad
				}
				cursorPos, ok := cursor[1].(float64)
				if !ok {
					errorMessage = "each filterCursor entry must have an integer id as its second entry"
					goto bad
				}
				_ = cursorPos
				cursorCell := new(int64)
				*cursorCell = int64(cursorPos)
				switch val := cursor[0].(type) {
				case float64:
					filterCursors[int64(val)] = cursorCell
				case string:
					filterCursors[val] = cursorCell
				}

			}

			switch protocolRequest.Kind {
			case "ping":
				err = c.WriteMessage(websocket.TextMessage, []byte("{\"kind\": \"pong\"}"))
				if err != nil {
					log.Println("write:", err)
					return
				}
				continue
			case "append":
				if !clientState.WritePermissionBit {
					errorMessage = "permission denied"
				} else {
					singleRow := make(map[string][]interface{})
					for k, v := range protocolRequest.Row {
						singleRow[k] = []interface{}{v}
					}
					err = serv.AppendRows(protocolRequest.Table, DataRows{
						Length: 1, Data: singleRow,
					})
					if err == nil {
						goto good
					}
					errorMessage = fmt.Sprintf("invalid append: %s", err)
				}
			case "appendBatch":
				if !clientState.WritePermissionBit {
					errorMessage = "permission denied"
				} else {
					err = serv.AppendRows(protocolRequest.Table, protocolRequest.Rows)
					if err == nil {
						goto good
					}
					errorMessage = fmt.Sprintf("invalid appendBatch: %s", err)
				}
			case "query":
				if _, ok := serv.Schema.Subscriptions[protocolRequest.Subscription]; ok {
					subState := SubscriptionCursor{
						SubscriptionName: protocolRequest.Subscription,
						Cursor:           protocolRequest.Cursor,
						FilterCursors:    filterCursors,
					}
					err = serv.CatchUpCursor(c, -1, &subState)
					// FIXME: Properly handle err here
					goto good
				} else {
					errorMessage = "unknown subscription"
				}
			case "subscribe":
				if _, ok := serv.Schema.Subscriptions[protocolRequest.Subscription]; ok {
					clientState.Cursors[protocolRequest.Token] = &SubscriptionCursor{
						SubscriptionName: protocolRequest.Subscription,
						Cursor:           protocolRequest.Cursor,
						FilterCursors:    filterCursors,
					}
					err = serv.CatchUpCursor(c, protocolRequest.Token, clientState.Cursors[protocolRequest.Token])
					// FIXME: Properly handle err here
					goto good
				} else {
					errorMessage = "unknown subscription"
				}
			case "unsubscribe":
				if _, ok := clientState.Cursors[protocolRequest.Token]; ok {
					delete(clientState.Cursors, protocolRequest.Token)
					goto good
				} else {
					errorMessage = "unknown subscription token"
				}
			case "getSchema":
				bytes, err := yaml.Marshal(serv.Schema)
				if err != nil {
					log.Println("yaml marshal:", err)
					return
				}
				bytes, err = json.Marshal(struct {
					Kind   string `json:"kind"`
					Schema string `json:"schema"`
				}{
					Kind:   "getKind",
					Schema: string(bytes),
				})
				if err != nil {
					log.Println("json marshal:", err)
					return
				}
				err = c.WriteMessage(websocket.TextMessage, bytes)
				if err != nil {
					log.Println("write:", err)
					return
				}
				continue
			case "setSchema":
				if !clientState.AdminPermissionBit {
					errorMessage = "permission denied"
				} else {
					errorMessage = "not implemented yet"
					//goto good
				}
			}

		bad:
			log.Printf("bad request: %#v", errorMessage)
			err = c.WriteMessage(websocket.TextMessage, []byte(
				fmt.Sprintf("{\"kind\": \"error\", \"message\": \"%s\"}", errorMessage),
			))
			if err != nil {
				log.Println("write:", err)
				return
			}
			continue

		good:
			err = c.WriteMessage(websocket.TextMessage, []byte("{\"kind\": \"ok\"}"))
			if err != nil {
				log.Println("write:", err)
				return
			}
		case <-wakeupChannel:
			//log.Println("wake up for wakeup channel")
			//c.WriteMessage()
		}
	}
}

func main() {
	configPath := flag.String("config", "streamdb-config.yaml", "Config file to load")
	schemaPath := flag.String("schema", "schema.yaml", "Schema of queries for the database")
	applySchema := flag.Bool("apply-schema", false, "If true we create any required tables")
	flag.Parse()

	// Parse the config file.
	configYaml, err := ioutil.ReadFile(*configPath)
	if err != nil {
		panic(err)
	}
	var config ServerConfig
	err = yaml.Unmarshal(configYaml, &config)
	if err != nil {
		panic(err)
	}
	if config.PostgresUrl == "" {
		panic("Need to set postgresUrl in config file")
	}
	if config.Host == "" {
		config.Host = "0.0.0.0:10203"
	}
	if config.ReadToken == "" {
		panic("Need to set ReadToken in config file")
	}
	if config.ReadWriteToken == "" {
		panic("Need to set ReadWriteToken in config file")
	}
	if config.AdminToken == "" {
		panic("Need to set AdminToken in config file")
	}

	// Parse the schema.
	schemaYaml, err := ioutil.ReadFile(*schemaPath)
	if err != nil {
		panic(err)
	}
	var schema ServerSchema
	err = yaml.Unmarshal(schemaYaml, &schema)
	if err != nil {
		panic(err)
	}

	// Connect to the database.
	db, err := sql.Open("postgres", config.PostgresUrl)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		panic(err)
	}

	if *applySchema {
		// Warning: Injection is hella possible in the code in this block.
		fmt.Println("Applying schema")
		for tableName, tableSpec := range schema.Tables {
			var sb strings.Builder
			fmt.Fprintf(&sb, "CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, created_at TIMESTAMP WITH TIME ZONE NOT NULL", tableName)
			for field, value := range tableSpec.Fields {
				switch value := value.(type) {
				case string:
					sqlType, ok := sqlTypeMapping[value]
					if !ok {
						panic(fmt.Sprintf("Unknown type in schema: %s", value))
					}
					fmt.Fprintf(&sb, ", %s %s", field, sqlType)
				case []interface{}:
					enumOptions := make([]string, 0)
					for _, x := range value {
						// Horrible injection issue here. TODO: Properly escape this.
						enumOptions = append(enumOptions, fmt.Sprintf("'%s'", x.(string)))
					}
					fmt.Fprintf(&sb, ", %s TEXT NOT NULL CHECK (%s in (%s))", field, field, strings.Join(enumOptions, ", "))
				default:
					panic(fmt.Sprintf("Unexpected value for field %#v in schema: %#v", field, value))
				}
			}
			fmt.Fprint(&sb, ")")
			fmt.Println(sb.String())
			_, err = db.Exec(sb.String())
			if err != nil {
				panic(err)
			}
		}
		return
	}

	fmt.Printf("StreamDB binding to %s\n", config.Host)
	server := Server{
		Config:        &config,
		Schema:        &schema,
		Database:      db,
		Clients:       make(map[chan string]*ClientState),
		Subscriptions: make(map[string]*SubscriptionState),
	}

	for subName := range schema.Subscriptions {
		err = server.RefreshSubscription(subName)
		if err != nil {
			log.Fatalf("Failed to start up: %v", err)
		}
	}

	http.HandleFunc("/ws", server.WebSocketEndpoint)
	http.HandleFunc("/api", server.WebSocketEndpoint)
	log.Fatal(http.ListenAndServe(config.Host, nil))
}
