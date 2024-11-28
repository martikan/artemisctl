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

	sub, err := conn.Subscribe(queueName, stomp.AckClientIndividual)
	if err != nil {
		log.Fatalf("cannot subscribe to %s: %v\n", queueName, err)
	}
	defer sub.Unsubscribe()

	log.Println("Waiting for messages...")

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

			fmt.Println(string(msg.Body))
			counter.Add(1)
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
