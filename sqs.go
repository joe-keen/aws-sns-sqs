package main

import (
	"fmt"

	"github.com/aws-sns-sqs/sqs"
)

func main() {
	reader, err := sqs.NewReader("test-topic", "test-sqs-service-queue")

	if err != nil {
		fmt.Printf("error creating SNS writer\n")
		return
	}

	for {
		event, receipt, err := reader.Read()
		if err != nil {
			fmt.Printf("error reading: %v\n", err)
			continue
		}

		err = reader.Commit(receipt)

		if err != nil {
			fmt.Printf("error commiting: %v\n", err)
			continue
		}

		fmt.Printf("\nmessage: %+v\n", event)
	}
}
