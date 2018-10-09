package sqs

import (
	"encoding/json"
	"fmt"

	"github.com/aws-sns-sqs/cncf"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	// "github.com/aws-sns-sqs/sns"
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

type Reader struct {
	SQS      *sqs.SQS
	QueueUrl *string
	QueueArn *string
	TopicArn *string
}

type messageBody struct {
	Type      string
	MessageId string
	Message   string
}

func (r *Reader) addPolicy() error {
	policy := PolicyDocument{
		Version: "2012-10-17",
		Id:      fmt.Sprintf("%v/sqs-sns-write-policy", *r.QueueArn),
		Statement: []StatementEntry{
			{
				Effect:    "Allow",
				Principal: "*",
				Action:    []string{"SQS:SendMessage"},
				Resource:  r.QueueArn,
				Condition: ConditionEntry{
					ArnEquals: map[string]*string{"aws:SourceArn": r.TopicArn},
				},
			},
		},
	}

	bytes, err := json.Marshal(&policy)

	if err != nil {
		fmt.Printf("error marshaling policy: %v\n", err)
		return err
	}

	_, err = r.SQS.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		Attributes: map[string]*string{"Policy": aws.String(string(bytes))},
		QueueUrl:   r.QueueUrl,
	})

	if err != nil {
		fmt.Printf("error setting queue attributes: %v\n", err)
		return err
	}

	return nil
}

func (r *Reader) createQueue(queueName string) error {
	sqsSvc := sqs.New(session.New())

	queueIdentifier := sqs.CreateQueueInput{}
	queueIdentifier.SetQueueName(queueName)
	queue, err := sqsSvc.CreateQueue(&queueIdentifier)

	if err != nil {
		fmt.Printf("error creating queue %v: %v\n", queueName, err)
		return err
	}

	attr, err := sqsSvc.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		AttributeNames: []*string{aws.String("QueueArn")},
		QueueUrl:       queue.QueueUrl,
	})

	if err != nil {
		fmt.Printf("Error getting queue attributes: %v\n", err)
		return err
	}

	r.SQS = sqsSvc
	r.QueueUrl = queue.QueueUrl
	r.QueueArn = attr.Attributes["QueueArn"]

	return nil
}

func (r *Reader) subscribeToSNS(topicName string) error {
	snsSvc := sns.New(session.New())

	topicIdentifier := sns.CreateTopicInput{}
	topicIdentifier.SetName(topicName)

	topic, err := snsSvc.CreateTopic(&topicIdentifier)

	if err != nil {
		fmt.Printf("error creating topic $v for queue: %v\n", topicName, err)
		return err
	}

	r.TopicArn = topic.TopicArn

	_, err = snsSvc.Subscribe(&sns.SubscribeInput{
		Endpoint: r.QueueArn,
		Protocol: aws.String("sqs"),
		TopicArn: r.TopicArn,
	})

	if err != nil {
		fmt.Printf("subscription error: %v\n", err)
		return err
	}

	err = r.addPolicy()

	if err != nil {
		fmt.Printf("error adding policy: %v\n", err)
		return err
	}

	return nil
}

func (r *Reader) Read() (cncf.Event, string, error) {
	for {
		result, err := r.SQS.ReceiveMessage(&sqs.ReceiveMessageInput{
			AttributeNames: []*string{
				aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
			},
			MessageAttributeNames: []*string{
				aws.String(sqs.QueueAttributeNameAll),
			},
			QueueUrl:            r.QueueUrl,
			MaxNumberOfMessages: aws.Int64(1),
			VisibilityTimeout:   aws.Int64(20),
			WaitTimeSeconds:     aws.Int64(1),
		})

		if err != nil {
			fmt.Println("error reading from %v queue: %v", r.QueueUrl, err)
			continue
		}

		if len(result.Messages) == 0 {
			fmt.Println("Received no messages")
			continue
		}

		receiptHandle := *result.Messages[0].ReceiptHandle

		body := messageBody{}

		err = json.Unmarshal([]byte(*result.Messages[0].Body), &body)

		if err != nil {
			fmt.Printf("unable to read message body: %v\n", err)
			return cncf.Event{}, receiptHandle, err
		}

		event := cncf.Event{}

		err = json.Unmarshal([]byte(body.Message), &event)

		if err != nil {
			fmt.Printf("unable to convert message to cncf event")
			return cncf.Event{}, receiptHandle, err
		}

		return event, receiptHandle, nil
	}
}

func (r *Reader) Commit(receipt string) error {
	_, err := r.SQS.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      r.QueueUrl,
		ReceiptHandle: aws.String(receipt),
	})

	if err != nil {
		fmt.Printf("error commiting receipt %v error: %v\n", receipt, err)
		return err
	}

	return nil
}

func NewReader(topicName, queueName string) (Reader, error) {
	r := Reader{}

	err := r.createQueue(queueName)

	if err != nil {
		return Reader{}, err
	}

	err = r.subscribeToSNS(topicName)

	if err != nil {
		return Reader{}, err
	}

	return r, nil
}
