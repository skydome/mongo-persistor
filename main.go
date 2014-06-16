package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"runtime"
	"time"

	"labix.org/v2/mgo"

	MQTT "git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
)

type Sensor struct {
	Holder   string
	Time     time.Time
	ServerID byte
	ID       byte
	Prefix   byte
	Code     byte
}

var session = new(mgo.Session)
var collection *mgo.Collection

func mgoSessionFinalizer(session *mgo.Session) {
	session.Close()
}

func init() {
	runtime.SetFinalizer(session, mgoSessionFinalizer)
	var err error
	//TODO: obtain MongoDB details from configuration
	session, err = mgo.Dial("db.skydome.io:27017")
	if err != nil {
		panic(err)
	}

	session.SetMode(mgo.Monotonic, true)
	collection = session.DB("sensors").C("raw")
}

var f MQTT.MessageHandler = func(client *MQTT.MqttClient, msg MQTT.Message) {
	//fmt.Printf("TOPIC: %s\n", msg.Topic())
	fmt.Println("Size of array : ", len(msg.Payload()))
	fmt.Println("MSG: ", msg.Payload())

	sensor := Sensor{hex.EncodeToString(msg.Payload()[:6]), time.Now(), msg.Payload()[6], msg.Payload()[7], msg.Payload()[8], msg.Payload()[9]}
	fmt.Println("sensor : ", sensor)
	collection.Insert(sensor)
}

func main() {
	opts := MQTT.NewClientOptions().SetBroker("tcp://api.skydome.io:1883").SetClientId("trial")
	opts.SetTraceLevel(MQTT.Off)
	opts.SetDefaultPublishHandler(f)

	c := MQTT.NewClient(opts)
	_, err := c.Start()
	if err != nil {
		panic(err)
	}

	filter, _ := MQTT.NewTopicFilter("skydome/#", 0)
	if receipt, err := c.StartSubscription(nil, filter); err != nil {
		fmt.Println(err)
		os.Exit(1)
	} else {
		<-receipt
	}

	for {
		time.Sleep(1 * time.Second)
	}
}
