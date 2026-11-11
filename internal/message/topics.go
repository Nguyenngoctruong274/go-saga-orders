package message

import (
	"fmt"
	"strings"
)

// Topics. Commands go to the participant that executes them, replies to
// the orchestrator. Every record is keyed by saga id, so the messages of
// one saga stay in one partition, in order.
const (
	TopicInventoryCommands = "inventory.commands"
	TopicPaymentCommands   = "payment.commands"
	TopicSagaReplies       = "saga.replies"
)

// DeadLetterSuffix is appended to a topic's name for the topic that holds
// the records its consumers could not process at all.
const DeadLetterSuffix = ".dlq"

// DeadLetter returns the dead-letter topic of topic.
func DeadLetter(topic string) string { return topic + DeadLetterSuffix }

// TopicSpec describes a topic to create.
type TopicSpec struct {
	Name       string
	Partitions int32
}

// Partitions per topic: the most consumers of one group that can share a
// topic. Six is enough for this demo and cheap to have; raising it later
// moves sagas between partitions, so it is sized up front.
const Partitions = 6

// Topics lists every topic the services use, with their dead-letter
// topics.
func Topics() []TopicSpec {
	var out []TopicSpec
	for _, t := range []string{TopicInventoryCommands, TopicPaymentCommands, TopicSagaReplies} {
		out = append(out, TopicSpec{Name: t, Partitions: Partitions}, TopicSpec{Name: DeadLetter(t), Partitions: 1})
	}
	return out
}

// CommandTopic returns the topic a command is sent to: the participant
// named before the first dot owns it.
func CommandTopic(command string) (string, error) {
	owner, _, ok := strings.Cut(command, ".")
	switch {
	case !ok:
	case owner == "inventory":
		return TopicInventoryCommands, nil
	case owner == "payment":
		return TopicPaymentCommands, nil
	}
	return "", fmt.Errorf("%w: no topic for command %q", ErrInvalid, command)
}

// ReplyType is the type of the reply to command.
func ReplyType(command string) string { return command + ".reply" }
