package main

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type PolicyDocument struct {
	Version   string
	Id        string
	Statement []StatementEntry
}

type StatementEntry struct {
	Effect    string
	Principal string
	Action    []string
	Resource  *string
	Condition ConditionEntry
}

type ConditionEntry struct {
	ArnEquals map[string]*string
}

func addPolicy(svc *sqs.SQS, queueUrl, queueARN, topicARN *string) error {
	policy := PolicyDocument{
		Version: "2012-10-17",
		Id:      fmt.Sprintf("%v/sqs-sns-write-policy", *queueARN),
		Statement: []StatementEntry{
			{
				Effect:    "Allow",
				Principal: "*",
				Action:    []string{"SQS:SendMessage"},
				Resource:  queueARN,
				Condition: ConditionEntry{
					ArnEquals: map[string]*string{"aws:SourceArn": topicARN},
				},
			},
		},
	}

	bytes, err := json.Marshal(&policy)

	if err != nil {
		fmt.Printf("error marshaling policy: %v\n", err)
		return err
	}

	fmt.Printf("json payload: %v\n", string(bytes))

	result, err := svc.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		Attributes: map[string]*string{"Policy": aws.String(string(bytes))},
		QueueUrl:   queueUrl,
	})

	if err != nil {
		fmt.Printf("error setting queue attributes: %v\n", err)
		return err
	}

	fmt.Printf("result: %v\n", result)

	return nil

}

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
	queueIdentifier.SetQueueName("sqs-test-queue-4")
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

	err = addPolicy(sqsSvc, queue.QueueUrl, queueAttrs.Attributes["QueueArn"], topic.TopicArn)

	if err != nil {
		fmt.Printf("error adding policy: %v\n", err)
		return
	}

	queueAttrs, err = sqsSvc.GetQueueAttributes(&sqs.GetQueueAttributesInput{
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
