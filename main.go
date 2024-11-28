package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
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

var (
	serverAddr string
	serverUser string
	serverPass string
	queueName  string
	method     string
	messages   collectionVar
	bodyType   string
	filePath   string
	helpFlag   = false

	closing = make(chan struct{})
)

func init() {
	flag.StringVar(&serverAddr, "b", "localhost:61613", "broker URL")
	flag.StringVar(&serverUser, "u", "", "username")
	flag.StringVar(&serverPass, "p", "", "password")
	flag.StringVar(&queueName, "q", "", "Destination queue")
	flag.StringVar(&method, "m", "C", "Method type. 'C' is consuming, 'P' is producing")
	flag.Var(&messages, "message", "Message to send")
	flag.StringVar(&bodyType, "t", "plain", "Type of message body. 'plain'=text/plain, 'json'=application/json")
	flag.StringVar(&filePath, "f", "", "Given file name to save data")
	flag.BoolVar(&helpFlag, "help", false, "Print help text")
	flag.Parse()

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

	sub, err := conn.Subscribe(queueName, stomp.AckClientIndividual)
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
			if msg == nil {
				fmt.Println("Subscription is already closed.")
				break loop
			}

			lastMessageTime = time.Now()

			if intoFile {
				_, err := file.WriteString(string(msg.Body) + "\n")
				if err != nil {
					log.Printf("Failed to write message to file: %v\n", err)
				}
			} else {
				fmt.Println(string(msg.Body))
			}

			counter.Add(1)

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
