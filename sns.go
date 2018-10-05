package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
)

func main() {
	//Create a session object to talk to SNS (also make sure you have your key and secret setup in your .aws/credentials file)
	svc := sns.New(session.New())

	topicIdentifier := sns.CreateTopicInput{}
	topicIdentifier.SetName("raw-events")

	topic, err := svc.CreateTopic(&topicIdentifier)

	if err != nil {
		fmt.Printf("error creating topic: %v\n", err)
	}

	count := 0
	for {

		params := &sns.PublishInput{
			Message:  aws.String(fmt.Sprintf("TestEvent-%v", count)),
			TopicArn: topic.TopicArn,
		}

		resp, err := svc.Publish(params) //Call to puclish the message

		if err != nil { //Check for errors
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
			return
		}

		// Pretty-print the response data.
		fmt.Println(resp)
		count++
		time.Sleep(10 * time.Second)
	}
}
