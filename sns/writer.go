package sns

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"

	"github.com/aws-sns-sqs/cncf"
)

type Writer struct {
	SNS      *sns.SNS
	TopicArn *string
}

func (w *Writer) Write(event cncf.Event) error {
	bytes, err := json.Marshal(&event)

	if err != nil {
		fmt.Printf("error converting event |%v| to json: %v\n", event, err)
		return err
	}

	params := &sns.PublishInput{
		Message:  aws.String(string(bytes)),
		TopicArn: w.TopicArn,
	}

	_, err = w.SNS.Publish(params)

	if err != nil {
		fmt.Printf("error writing to SNS: %v\n", err)
		return err
	}

	return nil
}

func NewWriter(topicName string) (Writer, error) {
	svc := sns.New(session.New())

	topicIdentifier := sns.CreateTopicInput{}
	topicIdentifier.SetName(topicName)
	topic, err := svc.CreateTopic(&topicIdentifier)

	if err != nil {
		fmt.Printf("error creating topic: %v\n", err)
		return Writer{}, err
	}

	w := Writer{
		SNS:      svc,
		TopicArn: topic.TopicArn,
	}

	return w, nil
}
