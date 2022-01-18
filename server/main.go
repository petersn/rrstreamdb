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
	"time"

	"github.com/gorilla/websocket"

	_ "github.com/lib/pq"
	"gopkg.in/yaml.v2"
)

/*
TODO:
[*] Add SQL polling
[ ] Make sure I'm appropriately locking everywhere
[ ] Plain HTTP handler
[ ] Add listen/unlisten commands that just stream new rows
[*] Deduplicate what gets loaded between subscriptions to the same table
[ ] Make wakeup smarter (only wake up appropriate listeners)
[ ] Implement binary search properly
[ ] Implement "limit" requests properly
[ ] Possibly implement a database -> config scraper
*/

const VERSION = "v0.1"
const PING_PERIOD = 30 * time.Second
const WRITE_WAIT = 10 * time.Second

var debugMode = false

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
	PostgresUrl        string  `yaml:"postgresUrl"`
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
	GroupByColumn string `yaml:"groupBy" json:"groupyBy,omitempty"` // "" means no grouping
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
}

func max(a, b int64) int64 {
	if a < b {
		return b
	}
	return a
}

func LOCKMESSAGE(x string) {
	if debugMode {
		fmt.Printf("\x1b[95m%s\x1b[0m\n", x)
	}
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
		}
	}

	// Create table pulls
	fmt.Printf("\x1b[93mPulling plan:\x1b[0m\n")
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
	startTime := time.Now()
	if debugMode && fullPrint {
		fmt.Printf("\x1b[93mPulling table:\x1b[0m %s\n", pull.TableName)
	}
	//subSpec := serv.Config.Subscriptions[subName]
	//subState := serv.Subscriptions[subName]

	var query string
	switch pull.PullKind {
	// Select the most recent row
	case PullJustNewest:
		query = fmt.Sprintf("SELECT * FROM %s WHERE id > %v ORDER BY id DESC LIMIT 1", pull.TableName, pull.MostRecentId)
	// Select just the most recent row of each group
	case PullNewestByGroup:
		query = fmt.Sprintf(
			"SELECT * FROM %s WHERE id > %v AND id IN (SELECT MAX(id) FROM %s GROUP BY %s) ORDER BY id",
			pull.TableName, pull.MostRecentId, pull.TableName, pull.GroupByColumn,
		)
	// Select all rows
	case PullAll:
		query = fmt.Sprintf("SELECT * FROM %s WHERE id > %v ORDER BY id", pull.TableName, pull.MostRecentId)
	}

	// Read all of the corresponding rows
	LOCKMESSAGE("RefreshPull - DatabaseMutex.Lock")
	serv.DatabaseMutex.Lock()
	defer func() {
		LOCKMESSAGE("RefreshPull - DatabaseMutex.Unlock")
		serv.DatabaseMutex.Unlock()
	}()
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
		fmt.Printf("\x1b[93mRefreshed:\x1b[0m %s - %v rows - took %.3f seconds\n", pull.DebugName, rowCount, time.Since(startTime).Seconds())
	}

	dataRows.Recompute()
	pull.MostRecentId = max(pull.MostRecentId, dataRows.MostRecentId)

	return serv.UpdateSubscriptions(pull.TableName, dataRows)

	/*
		groupByColumnIndex := -1
		if pull.GroupByColumn != "" {
			for i, columnName := range columns {
				if columnName == pull.GroupByColumn {
					groupByColumnIndex = i
				}
			}
			if groupByColumnIndex == -1 {
				return fmt.Errorf("Bad group by")
			}
		}

		serv.Mutex.Lock()
		defer serv.Mutex.Unlock()

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
			// Populate our stash of rows
			if subSpec.GroupByColumn == "" {
				if subSpec.MostRecent {
					for i, value := range dump {
						subState.RegularRows.Data[columns[i]] = []interface{}{value}
					}
				} else {
					for i, value := range dump {
						subState.RegularRows.Data[columns[i]] = append(subState.RegularRows.Data[columns[i]], value)
					}
				}
				subState.MostRecentId = max(subState.MostRecentId, subState.RegularRows.Recompute())
			} else {
				groupByValue := dump[groupByColumnIndex]
				if _, ok := subState.GroupByRows[groupByValue]; !ok {
					subState.GroupByRows[groupByValue] = &DataRows{
						Length: 0,
						Data:   make(map[string][]interface{}),
					}
				}
				ourGroup := subState.GroupByRows[groupByValue]
				if subSpec.MostRecent {
					for i, value := range dump {
						ourGroup.Data[columns[i]] = []interface{}{value}
					}
				} else {
					for i, value := range dump {
						ourGroup.Data[columns[i]] = append(ourGroup.Data[columns[i]], value)
					}
				}
				subState.MostRecentId = max(subState.MostRecentId, ourGroup.Recompute())
			}
			rowCount++
			if debugMode && fullPrint && ((rowCount < 10_000 && rowCount%1_000 == 0) ||
				(rowCount < 100_000 && rowCount%10_000 == 0) || rowCount%100_000 == 0) {
				fmt.Printf("    ... %v rows so far\n", rowCount)
			}
		}

		//fmt.Printf("%#v\n", subState)

		if rowCount > 0 {
			for channel, _ := range serv.Clients {
				channel <- WakeUpMessage{}
			}
		}
	*/
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

func (serv *Server) CatchUpCursor(
	conn *websocket.Conn,
	token int64,
	cursor *SubscriptionCursor,
	sendEmpty bool,
) error {
	//if debugMode {
	//	fmt.Printf("\x1b[91mCatch up cursor\x1b[0m: %v\n", cursor)
	//}
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
		//fmt.Printf("\x1b[91mUpdating to: %v\x1b[0m\n", subState.RegularRows.MostRecentId)
	} else {
		for cursorFilterValue, largestSeen := range cursor.FilterCursors {
			//fmt.Printf("Got here: %v %#v\n%#v\n", token, cursor, subState)
			ourGroup, ok := subState.GroupByRows[cursorFilterValue]
			if !ok {
				//pp.Print("Bad results:", subState.GroupByRows, cursorFilterValue)
				continue
			}
			addRows(*largestSeen, ourGroup)
			*cursor.FilterCursors[cursorFilterValue] = ourGroup.MostRecentId
			//if debugMode {
			//	fmt.Printf("\x1b[91mUpdating to: %v\x1b[0m\n", ourGroup.MostRecentId)
			//}
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
		err = WriteMessage(conn, bytes)
		if err != nil {
			return err
		}
	}
	return nil
}

func (serv *Server) AppendRows(tableName string, rowData DataRows) error {
	tableDesc, ok := serv.Config.Tables[tableName]
	if !ok {
		return fmt.Errorf("unknown table for append: %#v", tableName)
	}
	columnNames := make([]string, 0, len(tableDesc.Fields))
	for key := range tableDesc.Fields {
		columnNames = append(columnNames, key)
	}

	//if debugMode {
	//	fmt.Printf("\x1b[96mAppend rows: %v\x1b[0m\n", rowData.Length)
	//}

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
	//fmt.Println(sb.String())

	//if debugMode {
	//	fmt.Printf("\x1b[96mQuery formed\x1b[0m\n")
	//}

	ids := make([]interface{}, 0)
	createdAts := make([]interface{}, 0)
	LOCKMESSAGE("AppendRows - DatabaseMutex.Lock")
	serv.DatabaseMutex.Lock()
	defer func() {
		LOCKMESSAGE("AppendRows - DatabaseMutex.Unlock")
		serv.DatabaseMutex.Unlock()
	}()
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
	// It is somewhat sublte that this logic here is even correct, because we could get duplicate rows into
	// UpdateSubscriptions due to multiple pulls on the same table with differing group-by columns. However,
	// if this occurs then we necessarily don't have any non-most-recent (a.k.a. all) subscriptions on this
	// table, and therefore it's always safe to apply duplicate rows, so long as we ignore stale rows. Our
	// one precondition is that rowData be sorted by id.

	//if debugMode {
	//	fmt.Printf("\x1b[96mUpdating subscriptions!\x1b[0m\n")
	//}
	if rowData.Length == 0 {
		return nil
	}
	LOCKMESSAGE("UpdateSubscriptions - Mutex.Lock")
	serv.Mutex.Lock()
	LOCKMESSAGE("UpdateSubscriptions - Got lock!")
	defer func() {
		LOCKMESSAGE("UpdateSubscriptions - Mutex.Unlock")
		serv.Mutex.Unlock()
	}()

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
					if id >= subState.RegularRows.MostRecentId {
						subState.RegularRows.Data[columnName] = []interface{}{values[len(values)-1]}

					}
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
						Length: 0,
						Data:   make(map[string][]interface{}),
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

	//pp.Print(serv.Subscriptions)

	// Wake everyone up
	// TODO: Properly only wake up folks waiting on these tables, or maybe even these groups.
	//if debugMode {
	//	fmt.Printf("\x1b[95mSending wake ups\x1b[0m\n")
	//	pp.Print(serv.Clients)
	//}
	for channel, _ := range serv.Clients {
		channel <- WakeUpMessage{}
	}

	return nil
}

// FIXME: This endpoint doesn't do anything yet! All you can do is authenticate.
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
			Message: fmt.Sprint(err),
		})
		return
	}
	_ = clientState
	json.NewEncoder(w).Encode(struct {
		Kind    string `json:"kind"`
		Message string `json:"message"`
	}{
		Kind:    "success",
		Message: "ok",
	})
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

func (serv *Server) WebSocketEndpoint(w http.ResponseWriter, r *http.Request) {
	var bytes []byte
	upgrader.CheckOrigin = func(r *http.Request) bool { return true }
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}
	if debugMode {
		fmt.Printf("\x1b[91mOpen[%p]\x1b[0m\n", c)
	}
	ticker := time.NewTicker(PING_PERIOD)
	defer func() {
		if debugMode {
			fmt.Printf("\x1b[91mClose[%p]\x1b[0m\n", c)
		}
		c.Close()
		ticker.Stop()
	}()
	c.SetPongHandler(func(x string) error {
		//if debugMode {
		//	fmt.Printf("\x1b[93mRecv[%p]:\x1b[0m pong\n", c)
		//}
		c.SetReadDeadline(time.Now().Add(PING_PERIOD + 5*time.Second))
		return nil
	})

	// Read an initial auth message that must be equal to the secret password.
	c.SetReadDeadline(time.Now().Add(PING_PERIOD + 5*time.Second))
	mt, message, err := c.ReadMessage()
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
		err = WriteMessage(c, bytes)
		if err != nil {
			log.Println("write:", err)
		}
		return
	}

	err = WriteMessage(c, []byte(fmt.Sprintf("{\"kind\": \"auth\", \"version\": \"%s\"}", VERSION)))
	if err != nil {
		log.Println("write:", err)
		return
	}

	// TODO: Think carefully about this channel size. At size 0 we deadlock.
	// I think we might deadlock at any finite size in some situations, but maybe 1 suffices?
	wakeupChannel := make(chan WakeUpMessage, 128)
	LOCKMESSAGE("About to lock...")
	serv.Mutex.Lock()
	LOCKMESSAGE("Got lock!")
	serv.Clients[wakeupChannel] = clientState
	serv.Mutex.Unlock()
	defer func() {
		LOCKMESSAGE("Locking")
		serv.Mutex.Lock()
		delete(serv.Clients, wakeupChannel)
		LOCKMESSAGE("Unlocking")
		serv.Mutex.Unlock()
	}()

	messageChannel := make(chan string)
	go func() {
		for {
			//log.Println("loop on reading message")
			mt, message, err := c.ReadMessage()
			if err != nil || mt != websocket.TextMessage {
				log.Println("read:", err)
				break
			}
			if debugMode {
				fmt.Printf("\x1b[93mRecv[%p]:\x1b[0m %s\n", c, message)
			}
			if len(message) == 0 {
				log.Println("\x1b[91mlength zero message?\x1b[0m")
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
			if time.Now().Unix() > clientState.ValidityUntilUnix {
				WriteMessage(c, []byte("{\"kind\": \"error\", \"message\": \"auth expired\"}"))
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
				case bool:
					filterCursors[val] = cursorCell
				default:
					errorMessage = "each filterCursor key must be an integer, string, or boolean"
					goto bad
				}
			}

			switch protocolRequest.Kind {
			case "ping":
				err = WriteMessage(c, []byte("{\"kind\": \"pong\"}"))
				if err != nil {
					log.Println("write:", err)
					return
				}
				continue
			case "append":
				if !clientState.WritePermissionBit || serv.Config.ReadOnly {
					errorMessage = "permission denied"
				} else {
					singleRow := make(map[string][]interface{})
					for k, v := range protocolRequest.Row {
						singleRow[k] = []interface{}{v}
					}
					if err = serv.AppendRows(protocolRequest.Table, DataRows{
						Length: 1, Data: singleRow,
					}); err == nil {
						goto good
					}
					errorMessage = fmt.Sprintf("invalid append: %s", err)
				}
			case "appendBatch":
				if !clientState.WritePermissionBit || serv.Config.ReadOnly {
					errorMessage = "permission denied"
				} else {
					rowData := DataRows{
						Data: protocolRequest.Rows,
					}
					rowData.Recompute()
					err = serv.AppendRows(protocolRequest.Table, rowData)
					if err == nil {
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
					err = serv.CatchUpCursor(c, protocolRequest.Token, &subCursor, true)
					if err == nil {
						continue
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
					err = serv.CatchUpCursor(c, protocolRequest.Token, clientState.Cursors[protocolRequest.Token], true)
					if err == nil {
						continue
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
			case "getSchema":
				//bytes, err := yaml.Marshal(serv.Config)
				//if err != nil {
				//	log.Println("yaml marshal:", err)
				//	return
				//}
				bytes, err = json.Marshal(struct {
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
					return
				}
				err = WriteMessage(c, bytes)
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
				return
			}
			err = WriteMessage(c, bytes)
			if err != nil {
				log.Println("write:", err)
				return
			}
			continue

		good:
			// Token is an int64, I'm okay with not properly marshaling here.
			err = WriteMessage(c, []byte(fmt.Sprintf("{\"kind\": \"ok\", \"token\": %v}", protocolRequest.Token)))
			if err != nil {
				log.Println("write:", err)
				return
			}
		case <-wakeupChannel:
			if time.Now().Unix() > clientState.ValidityUntilUnix {
				WriteMessage(c, []byte("{\"kind\": \"error\", \"message\": \"auth expired\"}"))
				return
			}

			//if debugMode {
			//	fmt.Printf("\x1b[95mWake up: %p\x1b[0m\n", c)
			//}

			for token, subCursor := range clientState.Cursors {
				if err = serv.CatchUpCursor(c, token, subCursor, false); err != nil {
					log.Println("cursor:", err)
					return
				}
			}
			//log.Println("wake up for wakeup channel")
			//c.WriteMessage()
		case <-ticker.C:
			c.SetWriteDeadline(time.Now().Add(WRITE_WAIT))
			if err = c.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func main() {
	startUpTime := time.Now()
	configPath := flag.String("config", "streamdb-config.yaml", "Config file to load")
	makeAuthToken := flag.String("make-auth-token", "", "Make an auth token for a given claim")
	applySchema := flag.Bool("apply-schema", false, "If true we create any required tables")
	debugModeFlag := flag.Bool("debug-mode", false, "Print info on every message in and out")
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
	fmt.Printf("StreamDB binding to %s\n", config.Host)
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
	if config.EnableTLS {
		if config.CertFile == "" || config.KeyFile == "" {
			log.Fatal("You must specify certFile and keyFile in the config")
		}
		log.Fatal(http.ListenAndServeTLS(config.Host, config.CertFile, config.KeyFile, nil))
	} else {
		log.Fatal(http.ListenAndServe(config.Host, nil))
	}
}
