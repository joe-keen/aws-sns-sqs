package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func main() {
	//Create a session object to talk to SNS (also make sure you have your key and secret setup in your .aws/credentials file)
	svc := sns.New(session.New())

	topicIdentifier := sns.CreateTopicInput{}
	topicIdentifier.SetName("raw-events")

	topic, err := svc.CreateTopic(&topicIdentifier)

	if err != nil {
		fmt.Printf("error creating topic: %v\n", err)
		return
	}

	fmt.Printf("topic: %+v\n", topic)

	sqsSvc := sqs.New(session.New())

	queueIdentifier := sqs.CreateQueueInput{}
	queueIdentifier.SetQueueName("sqs-test-queue")
	queue, err := sqsSvc.CreateQueue(&queueIdentifier)

	if err != nil {
		fmt.Printf("error creating queue: %v\n", err)
		return
	}

	fmt.Printf("queue: %+v\n", queue)

	attributes, err := sqsSvc.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		AttributeNames: []*string{aws.String("QueueArn")},
		QueueUrl:       queue.QueueUrl,
	})

	if err != nil {
		fmt.Printf("Error getting queue attributes: %v\n", err)
		return
	}

	fmt.Printf("attributes: %+v\n", attributes)

	subscribeResult, err := svc.Subscribe(&sns.SubscribeInput{
		Endpoint: attributes.Attributes["QueueArn"],
		Protocol: aws.String("sqs"),
		TopicArn: topic.TopicArn,
	})

	if err != nil {
		fmt.Printf("subscription error: %v\n", err)
		return
	}

	fmt.Printf("subscribe result: %+v\n", subscribeResult)

	queueAttrs, err := sqsSvc.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		AttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl: queue.QueueUrl,
	})

	if err != nil {
		fmt.Printf("queue attribute error: %v\n", err)
		return
	}

	fmt.Printf("queue attributes: %v\n", queueAttrs)

	for {

		result, err := sqsSvc.ReceiveMessage(&sqs.ReceiveMessageInput{
			AttributeNames: []*string{
				aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
			},
			MessageAttributeNames: []*string{
				aws.String(sqs.QueueAttributeNameAll),
			},
			QueueUrl:            queue.QueueUrl,
			MaxNumberOfMessages: aws.Int64(1),
			VisibilityTimeout:   aws.Int64(20), // 20 seconds
			WaitTimeSeconds:     aws.Int64(1),
		})

		if err != nil {
			fmt.Println("Error", err)
			continue
		}

		if len(result.Messages) == 0 {
			fmt.Println("Received no messages")
			continue
		}

		fmt.Printf("result: %+v\n", result.Messages)

		deleteResult, err := sqsSvc.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      queue.QueueUrl,
			ReceiptHandle: result.Messages[0].ReceiptHandle,
		})

		if err != nil {
			fmt.Printf("delete error: %v\n", err)
			continue
		}

		fmt.Printf("message deleted: %v\n", deleteResult)
	}
}
