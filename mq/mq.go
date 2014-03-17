// IronMQ (elastic message queue) client library
package mq

import (
	"errors"
	"time"

	"github.com/iron-io/iron_go3/api"
	"github.com/iron-io/iron_go3/config"
)

type Timestamped struct {
	CreatedAt time.Time `json:"created_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

type Queue struct {
	Settings config.Settings
	Name     string
}

type QueueSubscriber struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
}

type QueueInfo struct {
	Name string `json:"name,omitempty"`
	Timestamped
	Size          int      `json:"size,omitempty"`
	ExpiresIn     int      `json:"expires_in,omitempty"`
	TotalMessages int      `json:"total_messages,omitempty"`
	Push          PushInfo `json:"push,omitempty"`
	Alerts        []Alert  `json:"alerts,omitempty"`
}

type PushInfo struct {
	Type         string            `json:"type"`
	RetriesDelay int               `json:"retries_delay,omitempty"`
	Retries      int               `json:"retries,omitempty"`
	Subscribers  []QueueSubscriber `json:"subscribers,omitempty"`
	ErrorQueue   string            `json:"error_queue,omitempty"`
}

type Alert struct {
	Type      string `json:"type"`
	Trigger   int    `json:"trigger"`
	Direction string `json:"direction"`
	Queue     string `json:"queue"`
	Snooze    int    `json:"snooze"`
}

type Message struct {
	Id string `json:"id,omitempty"`
	Timestamped
	Body string `json:"body"`
	// Timeout is the amount of time in seconds allowed for processing the message.
	Timeout int64 `json:"timeout,omitempty"`
	// Delay is the amount of time in seconds to wait before adding the message to the queue.
	Delay         int64     `json:"delay,omitempty"`
	ReservedUntil time.Time `json:"reserved_until,omitempty"`
	ReservedCount int       `json:"reserved_count,omitempty"`
	ReservationId string    `json:"reservation_id,omitempty"`
	q             Queue     // todo: shouldn't this be a pointer?
}

type PushStatus struct {
	Retried    int    `json:"retried"`
	StatusCode int    `json:"status_code"`
	Status     string `json:"status"`
}

type Subscriber struct {
	Retried    int    `json:"retried"`
	StatusCode int    `json:"status_code"`
	Status     string `json:"status"`
	URL        string `json:"url"`
}

type Subscription struct {
	PushType     string
	Retries      int
	RetriesDelay int
}

func New(queueName string) *Queue {
	return &Queue{Settings: config.Config("iron_mq"), Name: queueName}
}

func (q Queue) queues(s ...string) *api.URL { return api.Action(q.Settings, "queues", s...) }

func (q Queue) ListQueues(previous string, perPage int) (queues []Queue, err error) {
	out := []struct {
		Id         string
		Project_id string
		Name       string
	}{}

	err = q.queues().
		QueryAdd("previous", "%v", previous).
		QueryAdd("per_page", "%d", perPage).
		Req("GET", nil, &out)
	if err != nil {
		return
	}

	queues = make([]Queue, 0, len(out))
	for _, item := range out {
		queues = append(queues, Queue{
			Settings: q.Settings,
			Name:     item.Name,
		})
	}

	return
}

func (q Queue) Info() (QueueInfo, error) {
	qi := QueueInfo{}
	err := q.queues(q.Name).Req("GET", nil, &qi)
	return qi, err
}

func (q Queue) Update(qi QueueInfo) (QueueInfo, error) {
	out := QueueInfo{}
	err := q.queues(q.Name).Req("POST", qi, &out)
	return out, err
}

func (q Queue) Subscribe(subscription Subscription, subscribers ...string) (err error) {
	in := QueueInfo{
		Push: PushInfo{
			Type:         subscription.PushType,
			Retries:      subscription.Retries,
			RetriesDelay: subscription.RetriesDelay,
			Subscribers:  make([]QueueSubscriber, len(subscribers)),
		}}
	for i, subscriber := range subscribers {
		in.Push.Subscribers[i].URL = subscriber
	}
	return q.queues(q.Name).Req("POST", &in, nil)
}

func (q Queue) PushString(body string) (id string, err error) {
	ids, err := q.PushStrings(body)
	if err != nil {
		return
	}
	return ids[0], nil
}

// Push adds one or more messages to the end of the queue using IronMQ's defaults:
//	timeout - 60 seconds
//	delay - none
//
// Identical to PushMessages with Message{Timeout: 60, Delay: 0}
func (q Queue) PushStrings(bodies ...string) (ids []string, err error) {
	msgs := make([]*Message, 0, len(bodies))
	for _, body := range bodies {
		msgs = append(msgs, &Message{Body: body})
	}

	return q.PushMessages(msgs...)
}

func (q Queue) PushMessage(msg *Message) (id string, err error) {
	ids, err := q.PushMessages(msg)
	if err != nil {
		return
	}
	return ids[0], nil
}

func (q Queue) PushMessages(msgs ...*Message) (ids []string, err error) {
	in := struct {
		Messages []*Message `json:"messages"`
	}{
		Messages: msgs,
	}

	out := struct {
		IDs []string `json:"ids"`
		Msg string   `json:"msg"`
	}{}

	err = q.queues(q.Name, "messages").Req("POST", &in, &out)
	return out.IDs, err
}

// Get reserves a message from the queue.
// The message will not be deleted, but will be reserved until the timeout
// expires. If the timeout expires before the message is deleted, the message
// will be placed back onto the queue.
// As a result, be sure to Delete a message after you're done with it.
func (q Queue) Get() (msg *Message, err error) {
	msgs, err := q.GetN(1)
	if err != nil {
		return
	}

	if len(msgs) > 0 {
		msg = msgs[0]
	} else {
		err = errors.New("Couldn't get a single message")
	}

	return
}

// get N messages
func (q Queue) GetN(n int) (msgs []*Message, err error) {
	msgs, err = q.GetNWithTimeout(n, 0)

	return
}

func (q Queue) GetNWithTimeout(n, timeout int) (msgs []*Message, err error) {
	in := struct {
		N       int `'json:"n"`
		Timeout int `json:"timeout"`
	}{
		N: n,
		Timeout: timeout,
	}
	out := struct {
		Messages []*Message `json:"messages"`
	}{}

	err = q.queues(q.Name, "reservations").Req("POST", &in, &out)
	if err != nil {
		return
	}

	for _, msg := range out.Messages {
		msg.q = q
	}

	return out.Messages, nil
}

// Delete all messages in the queue
func (q Queue) Clear() (err error) {
	return q.queues(q.Name, "messages").Req("DELETE", &struct{}{}, nil)
}

// Delete message from queue
func (q Queue) DeleteMessage(msgId, reservationId string) (err error) {
	body := map[string]string{
		"reservation_id": reservationId,
	}
	return q.queues(q.Name, "messages", msgId).Req("DELETE", body, nil)
}

// Reset timeout of message to keep it reserved
func (q Queue) TouchMessage(msgId, reservationId string) (err error) {
	body := map[string]string{
		"reservation_id": reservationId,
	}
	return q.queues(q.Name, "messages", msgId, "touch").Req("POST", body, nil)
}

// Put message back in the queue, message will be available after +delay+ seconds.
func (q Queue) ReleaseMessage(msgId, reservationId string, delay int64) (err error) {
	body := struct {
		Delay         int64  `json:"delay"`
		ReservationId string `json:"reservation_id"`
	}{Delay: delay, ReservationId: reservationId}
	return q.queues(q.Name, "messages", msgId, "release").Req("POST", &body, nil)
}

func (q Queue) MessageSubscribers(msgId string) ([]*Subscriber, error) {
	out := struct {
		Subscribers []*Subscriber `json:"subscribers"`
	}{}
	err := q.queues(q.Name, "messages", msgId, "subscribers").Req("GET", nil, &out)
	return out.Subscribers, err
}

func (q Queue) MessageSubscribersPollN(msgId string, n int) ([]*Subscriber, error) {
	subs, err := q.MessageSubscribers(msgId)
	for {
		time.Sleep(100 * time.Millisecond)
		subs, err = q.MessageSubscribers(msgId)
		if err != nil {
			return subs, err
		}
		if len(subs) >= n && actualPushStatus(subs) {
			return subs, nil
		}
	}
	return subs, err
}

func actualPushStatus(subs []*Subscriber) bool {
	for _, sub := range subs {
		if sub.Status == "queued" {
			return false
		}
	}

	return true
}

// Delete message from queue
func (m Message) Delete() (err error) {
	return m.q.DeleteMessage(m.Id, m.ReservationId)
}

// Reset timeout of message to keep it reserved
func (m Message) Touch() (err error) {
	return m.q.TouchMessage(m.Id, m.ReservationId)
}

// Put message back in the queue, message will be available after +delay+ seconds.
func (m Message) Release(delay int64) (err error) {
	return m.q.ReleaseMessage(m.Id, m.ReservationId, delay)
}

func (m Message) Subscribers() (interface{}, error) {
	return m.q.MessageSubscribers(m.Id)
}
