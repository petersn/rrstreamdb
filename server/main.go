package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/yaml.v2"
)

/*
TODO:
[*] Add SQL polling
[*] I'm pretty sure my SQL polling implementation has a race with the main threads
[ ] Make sure I'm appropriately locking everywhere
[*] Plain HTTP handler
[ ] Add listen/unlisten commands that just stream new rows
[*] Deduplicate what gets loaded between subscriptions to the same table
[ ] Make wakeup smarter (only wake up appropriate listeners)
[*] Implement binary search properly
[ ] Implement "limit" requests properly
[ ] Possibly implement a database -> config scraper
*/

const VERSION = "v0.2.4"
const PING_PERIOD = 30 * time.Second
const WRITE_WAIT = 10 * time.Second

var debugMode = false
var startUpTime = time.Now()

var sqlTypeMapping = map[string]string{
	"Text":            "TEXT NOT NULL",
	"Float":           "DOUBLE PRECISION NOT NULL",
	"Integer":         "INTEGER NOT NULL",
	"Boolean":         "BOOLEAN NOT NULL",
	"JSON":            "JSONB NOT NULL",
	"NullableText":    "TEXT",
	"NullableFloat":   "DOUBLE PRECISION",
	"NullableInteger": "INTEGER",
	"NullableBoolean": "BOOLEAN",
	"NullableJSON":    "JSONB",
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
	DatabaseUrl        string  `yaml:"databaseUrl"`
	DatabaseDriver     string  `yaml:"databaseDriver"`
	Host               string  `yaml:"host"`
	HmacSecret         string  `yaml:"hmacSecret"`
	CertFile           string  `yaml:"certFile"`
	KeyFile            string  `yaml:"keyFile"`
	EnableTLS          bool    `yaml:"enableTLS"`
	DebugMode          bool    `yaml:"debugMode"`
	ReadOnly           bool    `yaml:"readOnly"`
	SQLPollingInterval float64 `yaml:"sqlPollingInterval"`

	// Schema part
	Tables map[string]struct {
		Fields map[string]interface{} `yaml:"fields" json:"fields"`
	} `yaml:"tables"`
	Subscriptions map[string]SubscriptionSpec `yaml:"subscriptions"`
}

type SubscriptionSpec struct {
	TableName     string `yaml:"table" json:"table"`
	GroupByColumn string `yaml:"groupBy" json:"groupBy,omitempty"` // "" means no grouping
	MostRecent    bool   `yaml:"mostRecent" json:"mostRecent"`
}

type ProtocolRequest struct {
	Kind          string                   `json:"kind"`
	Token         int64                    `json:"token"`
	Subscription  string                   `json:"subscription"`
	Table         string                   `json:"table"`
	Cursor        int64                    `json:"cursor"`
	FilterCursors [][]interface{}          `json:"groups"`
	Limit         int                      `json:"limit"`
	Row           map[string]interface{}   `json:"row"`
	Rows          map[string][]interface{} `json:"rows"`
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

const (
	PullJustNewest int64 = iota
	PullNewestByGroup
	PullAll
)

type TablePull struct {
	TableName     string
	PullKind      int64
	GroupByColumn string
	MostRecentId  int64
	DebugName     string
}

type ClientState struct {
	ValidityUntilUnix  int64
	WritePermissionBit bool
	AdminPermissionBit bool
	Cursors            map[int64]*SubscriptionCursor
}

type WakeUpMessage struct {
}

type Server struct {
	Config        *ServerConfig
	DatabaseMutex sync.Mutex
	Database      *sql.DB
	Mutex         sync.RWMutex
	Clients       map[chan WakeUpMessage]*ClientState
	Subscriptions map[string]*SubscriptionState
	TablePulls    []*TablePull
	RequestCount  int64
	BytesSent     int64
	BytesReceived int64
}

type ReplyFunction interface {
	Write(message []byte) error
}

func max(a, b int64) int64 {
	if a < b {
		return b
	}
	return a
}

func WriteMessage(conn *websocket.Conn, message []byte) error {
	if debugMode {
		fmt.Printf("\x1b[92mSend[%p]:\x1b[0m %s\n", conn, message)
	}
	conn.SetWriteDeadline(time.Now().Add(WRITE_WAIT))
	return conn.WriteMessage(websocket.TextMessage, message)
}

func (serv *Server) InitializeSubscriptions() {
	highestPullNeededByTable := make(map[string]int64)
	groupByColumnsByTable := make(map[string]map[string]struct{})

	for subName, subSpec := range serv.Config.Subscriptions {
		// Initialize the subscription state
		subState := &SubscriptionState{SubscriptionSpec: subSpec}
		if subSpec.GroupByColumn == "" {
			subState.RegularRows = DataRows{
				Length: 0,
				Data:   make(map[string][]interface{}),
			}
		} else {
			subState.GroupByRows = make(map[interface{}]*DataRows)
		}
		serv.Subscriptions[subName] = subState

		columns, ok := groupByColumnsByTable[subSpec.TableName]
		if !ok {
			columns = make(map[string]struct{})
			groupByColumnsByTable[subSpec.TableName] = columns
		}

		if !subSpec.MostRecent {
			// If we want all records (grouped or not) then we must pull all records
			highestPullNeededByTable[subSpec.TableName] = PullAll
		} else if subSpec.GroupByColumn != "" {
			// If we want the most recent within each group then we need to pull at least those,
			// and we can't get away with only pulling the most recent record overall
			highestPullNeededByTable[subSpec.TableName] = max(highestPullNeededByTable[subSpec.TableName], PullNewestByGroup)
			columns[subSpec.GroupByColumn] = struct{}{}
		} else {
			highestPullNeededByTable[subSpec.TableName] = max(highestPullNeededByTable[subSpec.TableName], PullJustNewest)
		}
	}

	// Create table pulls
	if debugMode {
		fmt.Printf("\x1b[93mPulling plan:\x1b[0m\n")
	}
	for tableName, pullKind := range highestPullNeededByTable {
		// If we need to pull the newest by group, then we need to create one table pull for each group by column
		if pullKind == PullNewestByGroup {
			for columnName := range groupByColumnsByTable[tableName] {
				serv.TablePulls = append(serv.TablePulls, &TablePull{
					TableName:     tableName,
					PullKind:      pullKind,
					GroupByColumn: columnName,
					MostRecentId:  -1,
					DebugName:     tableName + ":" + columnName,
				})
				if debugMode {
					fmt.Printf("\x1b[93m    ... pull each newest row from %s grouped by %s\x1b[0m\n", tableName, columnName)
				}
			}
		} else {
			serv.TablePulls = append(serv.TablePulls, &TablePull{
				TableName:    tableName,
				PullKind:     pullKind,
				MostRecentId: -1,
				DebugName:    tableName,
			})
			if debugMode {
				if pullKind == PullJustNewest {
					fmt.Printf("\x1b[93m    ... pull just the newest row from %s\x1b[0m\n", tableName)
				} else {
					fmt.Printf("\x1b[93m    ... pull all rows from %s\x1b[0m\n", tableName)
				}
			}
		}
	}
}

func (serv *Server) RefreshPull(pull *TablePull, fullPrint bool) error {
	// We must lock immediately as all access to TablePulls is synchronized by this mutex.
	serv.DatabaseMutex.Lock()
	defer serv.DatabaseMutex.Unlock()

	startTime := time.Now()
	if debugMode && fullPrint {
		fmt.Printf("\x1b[93mPulling table:\x1b[0m %s\n", pull.TableName)
	}
	var query string
	switch pull.PullKind {
	// Select the most recent row
	case PullJustNewest:
		query = fmt.Sprintf("SELECT * FROM %s WHERE id > %v ORDER BY id DESC LIMIT 1", pull.TableName, pull.MostRecentId)
	// Select just the most recent row of each group
	case PullNewestByGroup:
		query = fmt.Sprintf(
			"SELECT * FROM %s WHERE id IN (SELECT MAX(id) FROM %s WHERE id > %v GROUP BY %s) ORDER BY id",
			pull.TableName, pull.TableName, pull.MostRecentId, pull.GroupByColumn,
		)
	// Select all rows
	case PullAll:
		query = fmt.Sprintf("SELECT * FROM %s WHERE id > %v ORDER BY id", pull.TableName, pull.MostRecentId)
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

	dataRows := DataRows{
		Length: 0,
		Data:   make(map[string][]interface{}),
	}

	rowCount := 0
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
		for i, value := range dump {
			dataRows.Data[columns[i]] = append(dataRows.Data[columns[i]], value)
		}
		rowCount++
		if debugMode && fullPrint && ((rowCount < 10_000 && rowCount%1_000 == 0) ||
			(rowCount < 100_000 && rowCount%10_000 == 0) || rowCount%100_000 == 0) {
			fmt.Printf("    ... %v rows so far\n", rowCount)
		}
	}

	if debugMode && (fullPrint || rowCount > 0) {
		fmt.Printf(
			"\x1b[93mRefreshed:\x1b[0m %s - %v rows - took %.3f seconds\n",
			pull.DebugName, rowCount, time.Since(startTime).Seconds(),
		)
	}

	dataRows.Recompute()

	return serv.UpdateSubscriptions(pull.TableName, dataRows)
}

// Returns the index of the first row in rowData with an id strictly greater
// than largestSeen, or returns rowData.Length if there is no such row.
func binarySearchForNewRecords(largestSeen int64, rowData *DataRows) int {
	// Fast path is that there's no new data.
	if largestSeen >= rowData.MostRecentId {
		return rowData.Length
	}
	ids := rowData.Data["id"]
	// https://blog.nelhage.com/2015/08/indices-point-between-elements/
	lo, hi := 0, rowData.Length
	for lo < hi {
		// NB: Possibly I should do interpolation search here, because that would be absurdly effective.
		mid := (lo + hi) / 2
		value := ids[mid].(int64)
		if value < largestSeen {
			// If ids[mid] is too small then the first row must be strictly to the right
			lo = mid + 1
		} else if value == largestSeen {
			return mid + 1
		} else {
			// If ids[mid] is too large then the first row must be weakly to its left
			hi = mid
		}
	}
	return lo
}

func (serv *Server) CatchUpCursor(
	reply ReplyFunction,
	token int64,
	cursor *SubscriptionCursor,
	sendEmpty bool,
) error {
	retrievedData := make(map[string][]interface{})
	addRows := func(largestSeen int64, rowData *DataRows) {
		newIndex := binarySearchForNewRecords(largestSeen, rowData)
		if newIndex < rowData.Length {
			for fieldName, values := range rowData.Data {
				retrievedData[fieldName] = append(retrievedData[fieldName], values[newIndex:]...)
			}
		}
	}

	// Lookup the subscription state
	subState := serv.Subscriptions[cursor.SubscriptionName]
	if subState.SubscriptionSpec.GroupByColumn == "" {
		addRows(cursor.Cursor, &subState.RegularRows)
		cursor.Cursor = subState.RegularRows.MostRecentId
	} else {
		for cursorFilterValue, largestSeen := range cursor.FilterCursors {
			ourGroup, ok := subState.GroupByRows[cursorFilterValue]
			if !ok {
				continue
			}
			addRows(*largestSeen, ourGroup)
			*cursor.FilterCursors[cursorFilterValue] = ourGroup.MostRecentId
		}
	}

	if sendEmpty || len(retrievedData) > 0 {
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
		err = reply.Write(bytes)
		if err != nil {
			return err
		}
	}
	return nil
}

func (serv *Server) AppendRows(tableName string, rowData DataRows) error {
	rowData.Recompute()
	tableDesc, ok := serv.Config.Tables[tableName]
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

	// First we normalize the data to appropriate types.
	for columnName, columnValues := range rowData.Data {
		columnType, ok := tableDesc.Fields[columnName]
		if !ok {
			return errors.New("invalid column in data")
		}
		// Text, Float, Boolean, JSON, and enum types all already do the right thing.
		// However, integers in JSON will get deserialized as float64s, which we must undo.
		if columnType == "Integer" || columnType == "NullableInteger" {
			for i, value := range columnValues {
				columnValues[i] = int64(value.(float64))
			}
		}
	}
	// TODO: Make sure that every column is present, and they all have the same length

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

	ids := make([]interface{}, 0)
	createdAts := make([]interface{}, 0)
	serv.DatabaseMutex.Lock()
	defer serv.DatabaseMutex.Unlock()
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
	rowData.Recompute()

	return serv.UpdateSubscriptions(tableName, rowData)
}

// You must be holding the DatabaseMutex to call this function.
func (serv *Server) UpdateSubscriptions(tableName string, rowData DataRows) error {
	// It is somewhat subtle that this logic here is even correct, because we could get duplicate rows into
	// UpdateSubscriptions due to multiple pulls on the same table with differing group-by columns. However,
	// if this occurs then we necessarily don't have any non-most-recent (a.k.a. all) subscriptions on this
	// table, and therefore it's always safe to apply duplicate rows, so long as we ignore stale rows. Our
	// one precondition is that rowData be sorted by id.

	if rowData.Length == 0 {
		return nil
	}
	serv.Mutex.Lock()
	defer serv.Mutex.Unlock()

	// Sort of inefficient iteration.
	for _, pull := range serv.TablePulls {
		if pull.TableName == tableName {
			pull.MostRecentId = max(pull.MostRecentId, rowData.MostRecentId)
		}
	}

	// First we update all relevant subscriptions.
	for _, subState := range serv.Subscriptions {
		subSpec := subState.SubscriptionSpec
		if subSpec.TableName != tableName {
			continue
		}
		idsSlice := rowData.Data["id"]
		if subSpec.GroupByColumn == "" {
			id := idsSlice[len(idsSlice)-1].(int64)
			// We *must* ignore stale rows! See the block comment at the top of this function for an explanation.
			if subSpec.MostRecent && id <= subState.RegularRows.MostRecentId {
				continue
			}

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
			// Apply each row in turn, so we can split it out into the appropriate group
			for i := 0; i < rowData.Length; i++ {
				groupByValue := groupByColumnSlice[i]
				id := idsSlice[i].(int64)
				if _, ok := subState.GroupByRows[groupByValue]; !ok {
					subState.GroupByRows[groupByValue] = &DataRows{
						Length:       0,
						Data:         make(map[string][]interface{}),
						MostRecentId: -1,
					}
				}
				ourGroup := subState.GroupByRows[groupByValue]
				// We *must* ignore stale rows! See the block comment at the top of this function for an explanation.
				if subSpec.MostRecent && id <= ourGroup.MostRecentId {
					continue
				}

				for columnName, values := range rowData.Data {
					if subSpec.MostRecent {
						// If we want a single ungrouped most recent record then just immediately replace.
						ourGroup.Data[columnName] = []interface{}{values[i]}
					} else {
						// If we want all records grouped, then add this record to the group
						ourGroup.Data[columnName] = append(ourGroup.Data[columnName], values[i])
					}
				}
				ourGroup.Recompute()
			}
		}
	}

	// Wake everyone up
	// TODO: Properly only wake up folks waiting on these tables, or maybe even these groups.
	for channel, _ := range serv.Clients {
		channel <- WakeUpMessage{}
	}

	return nil
}

func (serv *Server) MakeClient(authToken []byte) (*ClientState, error) {
	// Split into claim-signature
	authTokenParts := bytes.SplitN(authToken, []byte("-"), 2)
	if len(authTokenParts) != 2 {
		return nil, errors.New("malformed auth token")
	}
	h := hmac.New(sha256.New, []byte(serv.Config.HmacSecret))
	h.Write(authTokenParts[0])
	// Get result and encode as hexadecimal string
	tag := hex.EncodeToString(h.Sum(nil)[:16])
	if !hmac.Equal([]byte(tag), authTokenParts[1]) {
		return nil, errors.New("bad auth token")
	}
	// Split the claim into permissions.lastValidTimestamp.random
	claimParts := strings.SplitN(string(authTokenParts[0]), ".", 3)
	if len(claimParts) != 3 {
		return nil, errors.New("malformed claim")
	}
	lastValidTimestamp, err := strconv.ParseInt(claimParts[1], 10, 64)
	if err != nil {
		return nil, errors.New("malformed claim")
	}
	if time.Now().Unix() > lastValidTimestamp {
		return nil, errors.New("expired claim")
	}

	log.Printf("connection: %s", authTokenParts[0])

	clientState := &ClientState{
		ValidityUntilUnix:  lastValidTimestamp,
		WritePermissionBit: false,
		AdminPermissionBit: false,
		Cursors:            make(map[int64]*SubscriptionCursor),
	}
	switch claimParts[0] {
	case "r":
		// Don't set any permissions
	case "rw":
		clientState.WritePermissionBit = true
	case "admin":
		clientState.WritePermissionBit = true
		clientState.AdminPermissionBit = true
	default:
		return nil, errors.New("invalid permission level")
	}
	return clientState, nil
}

func (serv *Server) HandleMessage(reply ReplyFunction, clientState *ClientState, message string) error {
	atomic.AddInt64(&serv.RequestCount, 1)
	atomic.AddInt64(&serv.BytesReceived, int64(len(message)))

	if time.Now().Unix() > clientState.ValidityUntilUnix {
		reply.Write([]byte(`{"kind": "error", "message": "auth expired"}`))
		return errors.New("auth expired")
	}

	bytes := []byte("")
	errorMessage := ""
	filterCursors := make(map[interface{}]*int64)
	var protocolRequest ProtocolRequest
	err := json.Unmarshal([]byte(message), &protocolRequest)
	if err != nil {
		log.Println("failed to parse:", err)
		errorMessage = fmt.Sprintf("failed to parse: %s", err)
		goto bad
	}

	for _, cursor := range protocolRequest.FilterCursors {
		if len(cursor) != 2 {
			errorMessage = `each filterCursor entry must be of length two, like: ["foo", 37]`
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
		case bool:
			filterCursors[val] = cursorCell
		default:
			errorMessage = "each filterCursor key must be an integer, string, or boolean"
			goto bad
		}
	}

	switch protocolRequest.Kind {
	case "ping":
		err = reply.Write([]byte(`{"kind": "pong"}`))
		if err != nil {
			log.Println("write:", err)
			return errors.New("write failed")
		}
		return nil
	case "append":
		if !clientState.WritePermissionBit || serv.Config.ReadOnly {
			errorMessage = "permission denied"
		} else {
			singleRow := make(map[string][]interface{})
			for k, v := range protocolRequest.Row {
				singleRow[k] = []interface{}{v}
			}
			if err = serv.AppendRows(protocolRequest.Table, DataRows{Data: singleRow}); err == nil {
				goto good
			}
			errorMessage = fmt.Sprintf("invalid append: %s", err)
		}
	case "appendBatch":
		if !clientState.WritePermissionBit || serv.Config.ReadOnly {
			errorMessage = "permission denied"
		} else {
			if err = serv.AppendRows(protocolRequest.Table, DataRows{Data: protocolRequest.Rows}); err == nil {
				goto good
			}
			errorMessage = fmt.Sprintf("invalid appendBatch: %s", err)
		}
	case "query":
		if _, ok := serv.Config.Subscriptions[protocolRequest.Subscription]; ok {
			subCursor := SubscriptionCursor{
				SubscriptionName: protocolRequest.Subscription,
				Cursor:           protocolRequest.Cursor,
				FilterCursors:    filterCursors,
				Limit:            protocolRequest.Limit,
			}
			err = serv.CatchUpCursor(reply, protocolRequest.Token, &subCursor, true)
			if err == nil {
				return nil
			}
			errorMessage = fmt.Sprintf("query failure: %s", err)
		} else {
			errorMessage = fmt.Sprintf("unknown subscription: %#v", protocolRequest.Subscription)
		}
	case "subscribe":
		if _, ok := serv.Config.Subscriptions[protocolRequest.Subscription]; ok {
			clientState.Cursors[protocolRequest.Token] = &SubscriptionCursor{
				SubscriptionName: protocolRequest.Subscription,
				Cursor:           protocolRequest.Cursor,
				FilterCursors:    filterCursors,
				Limit:            protocolRequest.Limit,
			}
			err = serv.CatchUpCursor(reply, protocolRequest.Token, clientState.Cursors[protocolRequest.Token], true)
			if err == nil {
				return nil
			}
			errorMessage = fmt.Sprintf("subscribe failure: %s", err)
		} else {
			errorMessage = fmt.Sprintf("unknown subscription: %#v", protocolRequest.Subscription)
		}
	case "unsubscribe":
		if _, ok := clientState.Cursors[protocolRequest.Token]; ok {
			delete(clientState.Cursors, protocolRequest.Token)
			goto good
		} else {
			errorMessage = "unknown subscription token"
		}
	case "health":
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		serv.Mutex.Lock()
		clientCount := len(serv.Clients)
		serv.Mutex.Unlock()
		bytes, err := json.Marshal(struct {
			Kind          string  `json:"kind"`
			Token         int64   `json:"token"`
			Version       string  `json:"version"`
			MemoryBytes   uint64  `json:"memoryBytes"`
			Clients       int     `json:"clients"`
			RequestCount  int64   `json:"requestCount"`
			BytesSent     int64   `json:"bytesSent"`
			BytesReceived int64   `json:"bytesReceived"`
			UptimeSeconds float64 `json:"uptimeSeconds"`
		}{
			Kind:          "health",
			Token:         protocolRequest.Token,
			Version:       VERSION,
			MemoryBytes:   m.Alloc,
			Clients:       clientCount,
			RequestCount:  serv.RequestCount,
			BytesSent:     serv.BytesSent,
			BytesReceived: serv.BytesReceived,
			UptimeSeconds: time.Since(startUpTime).Seconds(),
		})
		if err != nil {
			log.Println("json marshal:", err)
			return errors.New("json marshal failed")
		}
		err = reply.Write(bytes)
		if err != nil {
			log.Println("write:", err)
			return errors.New("write failed")
		}
		return nil
	case "getSchema":
		bytes, err := json.Marshal(struct {
			Kind          string      `json:"kind"`
			Token         int64       `json:"token"`
			Tables        interface{} `json:"tables"`
			Subscriptions interface{} `json:"subscriptions"`
		}{
			Kind:          "getSchema",
			Token:         protocolRequest.Token,
			Tables:        serv.Config.Tables,
			Subscriptions: serv.Config.Subscriptions,
		})
		if err != nil {
			log.Println("json marshal:", err)
			return errors.New("json marshal failed")
		}
		err = reply.Write(bytes)
		if err != nil {
			log.Println("write:", err)
			return errors.New("write failed")
		}
		return nil
	case "setSchema":
		if !clientState.AdminPermissionBit {
			errorMessage = "permission denied"
		} else {
			errorMessage = "not implemented yet"
		}
	}

bad:
	log.Printf("bad request: %#v", errorMessage)
	bytes, err = json.Marshal(struct {
		Kind    string `json:"kind"`
		Token   int64  `json:"token"`
		Message string `json:"message"`
	}{
		Kind:    "error",
		Token:   protocolRequest.Token,
		Message: errorMessage,
	})
	if err != nil {
		log.Println("marshal:", err)
		return errors.New("marshal failed")
	}
	err = reply.Write(bytes)
	if err != nil {
		log.Println("write:", err)
		return errors.New("write failed")
	}
	return nil

good:
	// Token is an int64, I'm okay with not properly marshaling here.
	err = reply.Write([]byte(fmt.Sprintf(`{"kind": "ok", "token": %v}`, protocolRequest.Token)))
	if err != nil {
		log.Println("write:", err)
		return errors.New("write failed")
	}
	return nil
}

type WebsocketReplyFunction struct {
	Serv *Server
	Conn *websocket.Conn
}

func (wsReplyFunc *WebsocketReplyFunction) Write(message []byte) error {
	atomic.AddInt64(&wsReplyFunc.Serv.BytesSent, int64(len(message)))
	return WriteMessage(wsReplyFunc.Conn, message)
}

func (serv *Server) WebSocketEndpoint(w http.ResponseWriter, r *http.Request) {
	var bytes []byte
	upgrader.CheckOrigin = func(r *http.Request) bool { return true }
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}
	if debugMode {
		fmt.Printf("\x1b[91mOpen[%p]\x1b[0m\n", conn)
	}
	ticker := time.NewTicker(PING_PERIOD)
	defer func() {
		if debugMode {
			fmt.Printf("\x1b[91mClose[%p]\x1b[0m\n", conn)
		}
		conn.Close()
		ticker.Stop()
	}()
	conn.SetPongHandler(func(x string) error {
		conn.SetReadDeadline(time.Now().Add(PING_PERIOD + 5*time.Second))
		return nil
	})

	// Read an initial auth message that must be equal to the secret password.
	conn.SetReadDeadline(time.Now().Add(PING_PERIOD + 5*time.Second))
	mt, message, err := conn.ReadMessage()
	if err != nil || mt != websocket.TextMessage {
		log.Print("bad auth message:", err)
		return
	}

	clientState, err := serv.MakeClient(message)
	if err != nil {
		bytes, err = json.Marshal(struct {
			Kind    string `json:"kind"`
			Message string `json:"message"`
		}{
			Kind:    "error",
			Message: err.Error(),
		})
		if err != nil {
			log.Println("marshal:", err)
			return
		}
		err = WriteMessage(conn, bytes)
		if err != nil {
			log.Println("write:", err)
		}
		return
	}

	err = WriteMessage(conn, []byte(fmt.Sprintf(`{"kind": "auth", "version": "%s"}`, VERSION)))
	if err != nil {
		log.Println("write:", err)
		return
	}

	// TODO: Think carefully about this channel size. At size 0 we deadlock.
	// I think we might deadlock at any finite size in some situations, but maybe 1 suffices?
	wakeupChannel := make(chan WakeUpMessage, 128)
	serv.Mutex.Lock()
	serv.Clients[wakeupChannel] = clientState
	serv.Mutex.Unlock()
	defer func() {
		serv.Mutex.Lock()
		delete(serv.Clients, wakeupChannel)
		serv.Mutex.Unlock()
	}()

	messageChannel := make(chan string)
	go func() {
		for {
			//log.Println("loop on reading message")
			mt, message, err := conn.ReadMessage()
			if err != nil || mt != websocket.TextMessage {
				log.Println("read:", err)
				break
			}
			if debugMode {
				fmt.Printf("\x1b[95mRecv[%p]:\x1b[0m %s\n", conn, message)
			}
			if len(message) == 0 {
				log.Println("\x1b[91mlength zero message?\x1b[0m")
				break
			}
			messageChannel <- string(message)
		}
		close(messageChannel)
	}()

	connReplyFunction := WebsocketReplyFunction{Serv: serv, Conn: conn}

	for {
		select {
		case message := <-messageChannel:
			if message == "" {
				return
			}
			if err = serv.HandleMessage(&connReplyFunction, clientState, message); err != nil {
				log.Println("error handling:", err)
				return
			}
		case <-wakeupChannel:
			if time.Now().Unix() > clientState.ValidityUntilUnix {
				WriteMessage(conn, []byte(`{"kind": "error", "message": "auth expired"}`))
				return
			}
			for token, subCursor := range clientState.Cursors {
				if err = serv.CatchUpCursor(&connReplyFunction, token, subCursor, false); err != nil {
					log.Println("cursor:", err)
					return
				}
			}
		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(WRITE_WAIT))
			if err = conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

type PlainReplyFunction struct {
	Serv *Server
	Dump []byte
}

func (plainReplyFunc *PlainReplyFunction) Write(message []byte) error {
	atomic.AddInt64(&plainReplyFunc.Serv.BytesSent, int64(len(message)))
	plainReplyFunc.Dump = message
	return nil
}

func (serv *Server) PlainEndpoint(w http.ResponseWriter, r *http.Request) {
	authString, ok := r.Header["Authorization"]
	if !ok || len(authString) != 1 {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	parts := strings.Split(authString[0], " ")
	if len(parts) != 2 || parts[0] != "Basic" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	authToken, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	usernameAndPasswordParts := strings.SplitN(string(authToken), ":", 2)
	if len(usernameAndPasswordParts) != 2 {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	clientState, err := serv.MakeClient([]byte(usernameAndPasswordParts[1]))
	if err != nil {
		json.NewEncoder(w).Encode(struct {
			Kind    string `json:"kind"`
			Message string `json:"message"`
		}{
			Kind:    "error",
			Message: err.Error(),
		})
		return
	}

	message, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("read:", err)
		return
	}

	plainReplyFunction := PlainReplyFunction{Serv: serv}
	if err = serv.HandleMessage(&plainReplyFunction, clientState, string(message)); err != nil {
		log.Println("error handling:", err)
	}
	w.Write(plainReplyFunction.Dump)
}

func main() {
	configPath := flag.String("config", "streamdb-config.yaml", "Config file to load")
	makeAuthToken := flag.String("make-auth-token", "", "Make an auth token for a given claim")
	applySchema := flag.Bool("apply-schema", false, "If true we create any required tables")
	debugModeFlag := flag.Bool("debug-mode", false, "Print info on every message in and out")
	version := flag.Bool("version", false, "Show version number: "+VERSION)
	flag.Parse()

	if *version {
		fmt.Printf("StreamDB version %s\n", VERSION)
		return
	}

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
	if config.DatabaseUrl == "" {
		panic("Need to set databaseUrl in config file")
	}
	if config.DatabaseDriver == "" {
		panic("Need to set databaseDriver in config file to one of: postgres, mysql, sqlite3")
	}
	if config.Host == "" {
		config.Host = "0.0.0.0:10203"
	}
	if config.HmacSecret == "" {
		panic("Need to set hmacSecret in config file")
	}

	if *makeAuthToken != "" {
		h := hmac.New(sha256.New, []byte(config.HmacSecret))
		h.Write([]byte(*makeAuthToken))
		tag := hex.EncodeToString(h.Sum(nil)[:16])
		fmt.Printf("%s-%s\n", *makeAuthToken, tag)
		return
	}
	debugMode = config.DebugMode || *debugModeFlag

	// Connect to the database.
	db, err := sql.Open(config.DatabaseDriver, config.DatabaseUrl)
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
		for tableName, tableSpec := range config.Tables {
			var sb strings.Builder
			fmt.Fprintf(&sb, "CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, created_at TIMESTAMP WITH TIME ZONE NOT NULL", tableName)
			for field, fieldType := range tableSpec.Fields {
				switch fieldType := fieldType.(type) {
				case string:
					sqlType, ok := sqlTypeMapping[fieldType]
					if !ok {
						panic(fmt.Sprintf("Unknown type in schema: %s", fieldType))
					}
					fmt.Fprintf(&sb, ", %s %s", field, sqlType)
				case []interface{}:
					enumOptions := make([]string, 0)
					for _, x := range fieldType {
						// Horrible injection issue here. TODO: Properly escape this.
						enumOptions = append(enumOptions, fmt.Sprintf("'%s'", x.(string)))
					}
					fmt.Fprintf(&sb, ", %s TEXT NOT NULL CHECK (%s in (%s))", field, field, strings.Join(enumOptions, ", "))
				default:
					panic(fmt.Sprintf("Unexpected type for field %#v in schema: %#v", field, fieldType))
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

	if debugMode {
		fmt.Print("\x1b[93mDebug mode\x1b[0m - ")
	}
	fmt.Printf("StreamDB version %s binding to %s\n", VERSION, config.Host)
	server := Server{
		Config:        &config,
		Database:      db,
		Clients:       make(map[chan WakeUpMessage]*ClientState),
		Subscriptions: make(map[string]*SubscriptionState),
	}

	server.InitializeSubscriptions()

	for _, pull := range server.TablePulls {
		err = server.RefreshPull(pull, true)
		if err != nil {
			log.Fatalf("Failed to start up: %v", err)
		}
	}
	if debugMode {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		fmt.Printf(
			"All %v subscriptions refreshed - %v MiB allocated - %.3f seconds to start up\n",
			len(config.Subscriptions), m.Alloc/1024/1024, time.Since(startUpTime).Seconds(),
		)
	}

	if config.SQLPollingInterval > 0 {
		fullPrint := true
		go func() {
			for {
				time.Sleep(time.Microsecond * time.Duration(1e6*config.SQLPollingInterval))
				for _, pull := range server.TablePulls {
					err = server.RefreshPull(pull, fullPrint)
					if err != nil {
						log.Printf("\x1b[91mFailed to pull %s:\x1b[0m %v", pull.DebugName, err)
					}
				}
				fullPrint = false
			}
		}()
	}

	http.HandleFunc("/ws", server.WebSocketEndpoint)
	http.HandleFunc("/api", server.PlainEndpoint)
	http.HandleFunc("/rest", server.PlainEndpoint)
	if config.EnableTLS {
		if config.CertFile == "" || config.KeyFile == "" {
			log.Fatal("You must specify certFile and keyFile in the config")
		}
		log.Fatal(http.ListenAndServeTLS(config.Host, config.CertFile, config.KeyFile, nil))
	} else {
		log.Fatal(http.ListenAndServe(config.Host, nil))
	}
}
