package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-stomp/stomp"
	"github.com/google/uuid"
)

type collectionVar []string

func (c *collectionVar) String() string {
	return fmt.Sprintf("%v", *c)
}

func (c *collectionVar) Set(value string) error {
	*c = append(*c, value)
	return nil
}

const (
	aqMsgTimestamp = "timestamp"
	aqMsgID        = "messageID"
	aqMsgTraceID   = "TRACE_ID"
	aqMsgSCS       = "SCS"
)

var (
	serverAddr   string
	serverUser   string
	serverPass   string
	queueName    string
	method       string
	messages     collectionVar
	bodyType     string
	filePath     string
	msgTraceID   string
	msgID        string
	apiName      string
	startTime    time.Time
	startTimeStr string
	endTime      time.Time
	endTimeStr   string
	helpFlag     = false

	closing = make(chan struct{})
)

func init() {
	flag.StringVar(&serverAddr, "b", "localhost:61613", "broker URL")
	flag.StringVar(&serverUser, "u", "", "username")
	flag.StringVar(&serverPass, "p", "", "password")
	flag.StringVar(&queueName, "q", "", "Destination queue")
	flag.StringVar(&method, "m", "C", "Method type. 'C' is consuming, 'P' is producing")
	flag.StringVar(&msgTraceID, "trace-id", "", "Filter for trace id")
	flag.StringVar(&msgID, "message-id", "", "Filter for message id")
	flag.StringVar(&apiName, "api", "", "Filter for api name")
	flag.StringVar(&startTimeStr, "startdate", "", "start time for filtering messages in RFC3339 format. e.g.: 2024-01-01T15:05:05Z")
	flag.StringVar(&endTimeStr, "enddate", "", "end time for filtering messages in RFC3339 format. e.g.: 2024-01-01T15:05:05Z")
	flag.Var(&messages, "message", "Message to send")
	flag.StringVar(&bodyType, "t", "plain", "Type of message body. 'plain'=text/plain, 'json'=application/json")
	flag.StringVar(&filePath, "f", "", "Given file name to save data")
	flag.BoolVar(&helpFlag, "help", false, "Print help text")
	flag.Parse()

	var err error

	if helpFlag || len(os.Args) == 1 {
		fmt.Fprintf(os.Stderr, "Usage of %s\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	if serverUser == "" {
		log.Fatalln("username must be provided!")
	}

	if serverPass == "" {
		log.Fatalln("password must be provided!")
	}

	if queueName == "" {
		log.Fatalln("queue must be provided!")
	}

	if startTimeStr != "" {
		startTime, err = time.Parse(time.RFC3339, startTimeStr)
		if err != nil {
			log.Fatalf("Invalid start time format: %v\n", err)
		}
	}

	if endTimeStr != "" {
		endTime, err = time.Parse(time.RFC3339, endTimeStr)
		if err != nil {
			log.Fatalf("Invalid end time format: %v\n", err)
		}
	}

}

func main() {
	conn, err := stomp.Dial("tcp", serverAddr,
		stomp.ConnOpt.Login(serverUser, serverPass),
		stomp.ConnOpt.Host(strings.Split(serverAddr, ":")[0]))
	if err != nil {
		log.Fatalln("cannot connect to server", err)
	}
	defer conn.Disconnect()

	var wg sync.WaitGroup

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
		<-signals
		log.Println("Initiating shutdown of consumer...")
		close(closing)
	}()

	switch method {
	case "C":
		wg.Add(1)
		go consumeMessages(conn, &wg)
	case "P":
		wg.Add(1)
		go sendMessages(conn, &wg)
	default:
		log.Fatalln("unknown method has been provided!")
	}

	wg.Wait()

}

func consumeMessages(conn *stomp.Conn, wg *sync.WaitGroup) {
	defer wg.Done()

	var counter atomic.Int32

	intoFile := filePath != ""

	var file *os.File
	defer file.Close()
	if intoFile {
		file = createFile(filePath)
	}

	sub, err := conn.Subscribe(queueName, stomp.AckClientIndividual, stomp.SubscribeOpt.Header("consumer-window-size", "-1"))
	if err != nil {
		log.Fatalf("cannot subscribe to %s: %v\n", queueName, err)
	}
	defer sub.Unsubscribe()

	log.Println("Waiting for messages...")

	lastMessageTime := time.Now()

loop:
	for {
		select {
		case <-closing:
			break loop
		case msg := <-sub.C:
			counter.Add(1)

			if msg.Err != nil {
				log.Println("Error during consuming messages:", msg.Err.Error())
				continue
			}

			if msg == nil {
				fmt.Println("Subscription is already closed.")
				break loop
			}

			// Filtering messages.
			if skipMessage(msg) {
				continue
			}

			lastMessageTime = time.Now()

			if intoFile {
				_, err := file.WriteString(string(msg.Body) + "\n")
				if err != nil {
					log.Printf("Failed to write message to file: %v\n", err)
				}
			} else {
				fmt.Printf("%s - %s\n", msg.Header.Get(aqMsgTraceID), string(msg.Body))
			}

			if intoFile && counter.Load()%1000 == 0 {
				fmt.Printf("%d messages processed so far...\n", counter.Load())
			}
		case <-time.After(5 * time.Second):
			// Check if no message has been consumed for the last 5 seconds
			// Only do echo if writing into file.
			if intoFile && time.Since(lastMessageTime) > 5*time.Second {
				log.Println("Waiting for messages...")
				lastMessageTime = time.Now()
			}
		}
	}

	log.Println("Finished consuming the messages! Total messages: ", counter.Load())
}

// skipMessage is a filter.
// If a given filter is not equal with the actual value,
// then the message will be skipped.
func skipMessage(msg *stomp.Message) bool {

	// Date interval filter
	if !startTime.IsZero() || !endTime.IsZero() {
		timestampStr := msg.Header.Get(aqMsgTimestamp)
		if timestampStr != "" {
			timeMillis, err := strconv.ParseInt(timestampStr, 10, 64)
			if err != nil {
				log.Printf("id:%s - invalid timestamp header\n", msg.Header.Get(aqMsgID))
				return true
			}

			// If start time is bigger then the actual timestamp then,
			// skip the message.
			if !startTime.IsZero() {
				if timeMillis < startTime.UnixMilli() {
					return true
				}
			}

			// If the end time is lower the the actual timestamp then,
			// skip the message.
			if !endTime.IsZero() {
				if timeMillis > endTime.UnixMilli() {
					return true
				}
			}

		} else {
			log.Printf("id:%s - timestamp has been not found in header!\n", msg.Header.Get(aqMsgID))
			return true
		}
	}

	// Trace ID filter
	if msgTraceID != "" {
		id := msg.Header.Get(aqMsgTraceID)
		if msgTraceID == "" {
			log.Printf("id:%s - %s property is missing\n", aqMsgTraceID, msg.Header.Get(aqMsgTraceID))
			return true
		}

		if msgTraceID != id {
			return true
		}
	}

	// Message ID filter
	if msgID != "" {
		id := msg.Header.Get(aqMsgID)
		if msgID == "" {
			log.Printf("id:%s - %s property is missing\n", aqMsgTraceID, msg.Header.Get(aqMsgID))
			return true
		}

		if msgID != id {
			return true
		}
	}
	// API name filter
	if apiName != "" {
		msgAPI := msg.Header.Get(aqMsgSCS)
		if msgAPI == "" {
			log.Printf("id:%s - %s property is missing\n", aqMsgSCS, msg.Header.Get(aqMsgSCS))
			return true
		}

		if msgAPI != apiName {
			return true
		}
	}

	return false
}

func sendMessages(conn *stomp.Conn, wg *sync.WaitGroup) {
	defer wg.Done()

	var err error
	var counter atomic.Int32

	if len(messages) == 0 {
		log.Fatalln("No message was provided to send!")
		return
	}

	isJSON := false
	var messageBodyType string
	switch bodyType {
	case "plain":
		messageBodyType = "text/plain"
	case "json":
		messageBodyType = "application/json"
	default:
		log.Fatalln("Invalid message body type was provided:", bodyType)
	}

	for _, msg := range messages {
		var data []byte
		if isJSON {
			// process json message
			data, err = json.Marshal(&msg)
			if err != nil {
				log.Printf("Failed to marshal message to JSON. Skipping...: %v\n", err)
				continue
			}
		} else {
			// process plain text message
			data = []byte(msg)
		}

		messageID := uuid.NewString()

		// Send json message
		err = conn.Send(
			queueName,
			messageBodyType,
			data,
			stomp.SendOpt.Header("persistent", "true"),
			stomp.SendOpt.Header("messageId", messageID))
		if err != nil {
			log.Println("Failed to send message:", err)
		}

		counter.Add(1)
	}

	log.Println("Finished sending messages! Total sent: ", counter.Load())
}

func createFile(path string) *os.File {
	// Open the file to write messages
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open file for writing: %v\n", err)
	}

	return file
}
