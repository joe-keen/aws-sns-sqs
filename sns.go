package main

import (
	"fmt"
	"time"

	"github.com/aws-sns-sqs/cncf"
	"github.com/aws-sns-sqs/sns"
)

func main() {
	writer, err := sns.NewWriter("test-topic")

	if err != nil {
		fmt.Printf("error creating SNS writer\n")
		return
	}

	count := 0

	for {
		event := cncf.New("testEvent", "A",
			map[string]string{"count": fmt.Sprintf("%v", count)})

		err := writer.Write(event)
		if err != nil {
			fmt.Printf("error writing: %v\n", err)
			return
		}
		count++
		time.Sleep(10 * time.Second)
	}
}
