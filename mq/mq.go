// IronMQ (elastic message queue) client library
package mq

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/iron-io/iron_go3/api"
	"github.com/iron-io/iron_go3/config"
)

type Timestamped struct {
	CreatedAt time.Time `json:"created_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

// A Queue is the client's idea of a queue, sufficient for getting
// information for the queue with given Name at the server configured
// with Settings. See mq.New()
type Queue struct {
	Settings config.Settings `json:"-"`
	Name     string          `json:"name"`
}

// When used for create/update, Size and TotalMessages will be omitted.
type QueueInfo struct {
	Name string `json:"name"`

	Size          int `json:"size"`
	TotalMessages int `json:"total_messages"`

	MessageExpiration int       `json:"message_expiration"`
	MessageTimeout    int       `json:"message_timeout"`
	Type              *string   `json:"type,omitempty"`
	Push              *PushInfo `json:"push,omitempty"`
	Alerts            []Alert   `json:"alerts,omitempty"`
}

type PushInfo struct {
	RetriesDelay int               `json:"retries_delay,omitempty"`
	Retries      int               `json:"retries,omitempty"`
	Subscribers  []QueueSubscriber `json:"subscribers,omitempty"`
	ErrorQueue   string            `json:"error_queue,omitempty"`
}

type QueueSubscriber struct {
	Name    string            `json:"name"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"` // HTTP headers
}

type Alert struct {
	Type      string `json:"type"`
	Trigger   int    `json:"trigger"`
	Direction string `json:"direction"`
	Queue     string `json:"queue"`
	Snooze    int    `json:"snooze"`
}

// Message is dual purpose, as it represents a returned message and also
// can be used for creation. For creation, only Body and Delay are valid.
// Delay will not be present in returned message.
type Message struct {
	Id            string    `json:"id,omitempty"`
	Body          string    `json:"body"`
	Delay         int64     `json:"delay,omitempty"` // time in seconds to wait before enqueue, default 0
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

func ErrQueueNotFound(err error) bool {
	return err.Error() == "404 Not Found: Queue not found"
}

// New uses the configuration specified in an iron.json file or environment variables
// to return a Queue object capable of acquiring information about or modifying the queue
// specified by queueName.
func New(queueName string) Queue {
	return Queue{Settings: config.Config("iron_mq"), Name: queueName}
}

// ConfigNew uses the specified settings over configuration specified in an iron.json file or
// environment variables to return a Queue object capable of acquiring information about or
// modifying the queue specified by queueName.
func ConfigNew(queueName string, settings *config.Settings) Queue {
	return Queue{Settings: config.ManualConfig("iron_mq", settings), Name: queueName}
}

// Will create a new queue, all fields are optional.
// Queue type cannot be changed.
func CreateQueue(queueName string, queueInfo QueueInfo) (QueueInfo, error) {
	info := queueInfo
	info.Name = queueName
	return ConfigCreateQueue(info, nil)
}

// Will create a new queue, all fields are optional.
// Queue type cannot be changed.
func ConfigCreateQueue(queueInfo QueueInfo, settings *config.Settings) (QueueInfo, error) {
	if queueInfo.Name == "" {
		return QueueInfo{}, errors.New("Name of queue is empty")
	}

	url := api.Action(config.ManualConfig("iron_mq", settings), "queues", queueInfo.Name)

	in := struct {
		Queue QueueInfo `json:"queue"`
	}{
		Queue: queueInfo,
	}

	var out struct {
		Queue QueueInfo `json:"queue"`
	}

	err := url.Req("PUT", in, &out)
	return out.Queue, err
}

// List will get a listQueues of all queues for the configured project, paginated 30 at a time
// For paging or filtering, see ListPage and Filter.
// Uses the default configuration settings.
func List() ([]Queue, error) {
	return ConfigList(config.Config("iron_mq"))
}

// ConfigList is like List but will allow specifying manual configuration settings.
func ConfigList(settings config.Settings) ([]Queue, error) {
	return listQueues("", "", 0, settings)
}

// ListPage is like List, but will allow specifying a page length and pagination
// To get the first page, let prev = "".
// To get the second page, use the name of the last queue on the first page as "prev".
// Uses the default configuration settings.
func ListPage(prev string, perPage int) ([]Queue, error) {
	return ConfigListPage(prev, perPage, config.Config("iron_mq"))
}

// ConfigListPage is like ListPage, but will allow specifying manual configuration settings.
func ConfigListPage(prev string, perPage int, settings config.Settings) ([]Queue, error) {
	return listQueues("", prev, perPage, settings)
}

// Filter is like List, but will only return queues with the specified prefix.
// Uses the default configuration settings.
func Filter(prefix string) ([]Queue, error) {
	return FilterConfig(prefix, config.Config("iron_mq"))
}

// FilterConfig is like Filter, but will allow specifying manual configuration settings.
func FilterConfig(prefix string, settings config.Settings) ([]Queue, error) {
	return listQueues(prefix, "", 0, settings)
}

// Like ListPage, but with an added filter.
// Uses the default configuration settings.
func FilterPage(prefix, prev string, perPage int) ([]Queue, error) {
	return FilterPageConfig(prefix, prev, perPage, config.Config("iron_mq"))
}

// Like FilterPage, but will allow specifying manual configuration settings.
func FilterPageConfig(prefix, prev string, perPage int, settings config.Settings) ([]Queue, error) {
	return listQueues(prefix, prev, perPage, settings)
}

func listQueues(prefix, prev string, perPage int, settings config.Settings) ([]Queue, error) {
	var out struct {
		Queues []Queue `json:"queues"`
	}
	url := api.Action(settings, "queues")

	if prev != "" {
		url.QueryAdd("previous", "%v", prev)
	}
	if prefix != "" {
		url.QueryAdd("prefix", "%v", prefix)
	}
	if perPage != 0 {
		url.QueryAdd("per_page", "%d", perPage)
	}

	err := url.Req("GET", nil, &out)
	if err != nil {
		return nil, err
	}

	return out.Queues, nil
}

func (q Queue) queues(s ...string) *api.URL { return api.Action(q.Settings, "queues", s...) }

func (q *Queue) UnmarshalJSON(data []byte) error {
	var name struct {
		Name string `json:"name"`
	}
	err := json.Unmarshal(data, &name)
	q.Name = name.Name
	q.Settings = config.Config("iron_mq") // TODO could maybe cache this in config, map[$PWD]config, if $PWD changes, update config
	return err
}

// Will return information about a queue, could also be used to check existence.
// TODO make QueueNotExist err
func (q Queue) Info() (QueueInfo, error) {
	var out struct {
		QI QueueInfo `json:"queue"`
	}
	err := q.queues(q.Name).Req("GET", nil, &out)
	return out.QI, err
}

// Will create or update a queue, all QueueInfo fields are optional.
// Queue type cannot be changed.
func (q Queue) Update(queueInfo QueueInfo) (QueueInfo, error) {
	var out struct {
		QI QueueInfo `json:"queue"`
	}
	in := struct {
		QI QueueInfo `json:"queue"`
	}{
		QI: queueInfo,
	}

	err := q.queues(q.Name).Req("PATCH", in, &out)
	return out.QI, err
}

func (q Queue) Delete() error {
	return q.queues(q.Name).Req("DELETE", nil, nil)
}

// PushString enqueues a message with body specified and no delay.
func (q Queue) PushString(body string) (id string, err error) {
	ids, err := q.PushStrings(body)
	if err != nil {
		return
	}
	return ids[0], nil
}

// PushStrings enqueues messages with specified bodies and no delay.
func (q Queue) PushStrings(bodies ...string) (ids []string, err error) {
	msgs := make([]Message, len(bodies))
	for i, body := range bodies {
		msgs[i] = Message{Body: body}
	}

	return q.PushMessages(msgs...)
}

// PushMessage enqueues a message.
func (q Queue) PushMessage(msg Message) (id string, err error) {
	ids, err := q.PushMessages(msg)
	if err != nil {
		return "", err
	} else if len(ids) < 1 {
		return "", errors.New("didn't receive message ID for pushing message")
	}
	return ids[0], err
}

// PushMessages enqueues each message in order.
func (q Queue) PushMessages(msgs ...Message) (ids []string, err error) {
	in := struct {
		Messages []Message `json:"messages"`
	}{
		Messages: msgs,
	}

	var out struct {
		IDs []string `json:"ids"`
		Msg string   `json:"msg"` // TODO get rid of this on server and here, too.
	}

	err = q.queues(q.Name, "messages").Req("POST", &in, &out)
	return out.IDs, err
}

// Peek first 30 messages on queue.
func (q Queue) Peek() ([]Message, error) {
	return q.PeekN(30)
}

// Peek with N, max 100.
func (q Queue) PeekN(n int) ([]Message, error) {
	var out struct {
		Messages []Message `json:"messages"`
	}

	err := q.queues(q.Name, "messages").
		QueryAdd("n", "%d", n).
		Req("GET", nil, &out)

	for i, _ := range out.Messages {
		out.Messages[i].q = q
	}

	return out.Messages, err
}

// Reserves a message from the queue.
// The message will not be deleted, but will be reserved until the timeout
// expires. If the timeout expires before the message is deleted, the message
// will be placed back onto the queue.
// As a result, be sure to Delete a message after you're done with it.

func (q Queue) Reserve() (msg *Message, err error) {
	msgs, err := q.GetN(1)
	if len(msgs) > 0 {
		return &msgs[0], err
	}
	return nil, err
}

// ReserveN reserves multiple messages from the queue.
func (q Queue) ReserveN(n int) ([]Message, error) {
	return q.LongPoll(n, 60, 0, false)
}

// Get reserves a message from the queue.
// Deprecated, use Reserve instead.
func (q Queue) Get() (msg *Message, err error) {
	return q.Reserve()
}

// GetN is Get for N.
// Deprecated, use ReserveN instead.
func (q Queue) GetN(n int) ([]Message, error) {
	return q.ReserveN(n)
}

// TODO deprecate for LongPoll?
func (q Queue) GetNWithTimeout(n, timeout int) ([]Message, error) {
	return q.LongPoll(n, timeout, 0, false)
}

// Pop will get and delete a message from the queue.
func (q Queue) Pop() (msg Message, err error) {
	msgs, err := q.PopN(1)
	if len(msgs) > 0 {
		msg = msgs[0]
	}
	return msg, err
}

// PopN is Pop for N.
func (q Queue) PopN(n int) ([]Message, error) {
	return q.LongPoll(n, 0, 0, true)
}

// LongPoll is the long form for Get, Pop, with all options available.
// If wait = 0, then LongPoll is simply a get, otherwise, the server
// will poll for n messages up to wait seconds (max 30).
// If delete is specified, then each message will be deleted instead
// of being put back onto the queue.
func (q Queue) LongPoll(n, timeout, wait int, delete bool) ([]Message, error) {
	in := struct {
		N       int  `json:"n"`
		Timeout int  `json:"timeout"`
		Wait    int  `json:"wait"`
		Delete  bool `json:"delete"`
	}{
		N:       n,
		Timeout: timeout,
		Wait:    wait,
		Delete:  delete,
	}
	var out struct {
		Messages []Message `json:"messages"` // TODO don't think we need pointer here
	}

	err := q.queues(q.Name, "reservations").Req("POST", &in, &out)

	for i, _ := range out.Messages {
		out.Messages[i].q = q
	}

	return out.Messages, err
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

// Delete multiple messages by id
func (q Queue) DeleteMessages(ids []string) error {
	values := make([]map[string]string, len(ids))

	for i, val := range ids {
		element := map[string]string{
			"id": val,
		}
		values[i] = element
	}
	in := struct {
		Ids []map[string]string `json:"ids"`
	}{
		Ids: values,
	}
	return q.queues(q.Name, "messages").Req("DELETE", in, nil)
}

// Delete multiple reserved messages from the queue
func (q Queue) DeleteReservedMessages(messages []Message) error {
	values := make([]map[string]string, len(messages))

	for i, val := range messages {
		element := map[string]string{
			"id":             val.Id,
			"reservation_id": val.ReservationId,
		}
		values[i] = element
	}
	in := struct {
		Ids []map[string]string `json:"ids"`
	}{
		Ids: values,
	}
	return q.queues(q.Name, "messages").Req("DELETE", in, nil)
}

// Reset timeout of message to keep it reserved
func (q Queue) TouchMessage(msgId, reservationId string) (string, error) {
	return q.TouchMessageFor(msgId, reservationId, 0)
}

// Reset timeout of message to keep it reserved
func (q Queue) TouchMessageFor(msgId, reservationId string, timeout int) (string, error) {
	in := struct {
		Timeout       int    `json:"timeout,omitempty"`
		ReservationId string `json:"reservation_id,omitempty"`
	}{ReservationId: reservationId}
	if timeout > 0 {
		in.Timeout = timeout
	}
	out := &Message{}
	err := q.queues(q.Name, "messages", msgId, "touch").Req("POST", in, out)
	return out.ReservationId, err
}

// Put message back in the queue, message will be available after +delay+ seconds.
func (q Queue) ReleaseMessage(msgId, reservationId string, delay int64) (err error) {
	body := struct {
		Delay         int64  `json:"delay"`
		ReservationId string `json:"reservation_id"`
	}{Delay: delay, ReservationId: reservationId}
	return q.queues(q.Name, "messages", msgId, "release").Req("POST", &body, nil)
}

func (q Queue) MessageSubscribers(msgId string) ([]Subscriber, error) {
	out := struct {
		Subscribers []Subscriber `json:"subscribers"`
	}{}
	err := q.queues(q.Name, "messages", msgId, "subscribers").Req("GET", nil, &out)
	return out.Subscribers, err
}

func (q Queue) AddSubscribers(subscribers ...QueueSubscriber) error {
	collection := struct {
		Subscribers []QueueSubscriber `json:"subscribers,omitempty"`
	}{
		Subscribers: subscribers,
	}
	return q.queues(q.Name, "subscribers").Req("POST", &collection, nil)
}

func (q Queue) ReplaceSubscribers(subscribers ...QueueSubscriber) error {
	collection := struct {
		Subscribers []QueueSubscriber `json:"subscribers,omitempty"`
	}{
		Subscribers: subscribers,
	}
	return q.queues(q.Name, "subscribers").Req("PUT", &collection, nil)
}

func (q Queue) RemoveSubscribers(subscribers ...string) error {
	collection := make([]QueueSubscriber, len(subscribers))
	for i, subscriber := range subscribers {
		collection[i].Name = subscriber
	}
	return q.RemoveSubscribersCollection(collection...)
}

func (q Queue) RemoveSubscribersCollection(subscribers ...QueueSubscriber) error {
	collection := struct {
		Subscribers []QueueSubscriber `json:"subscribers,omitempty"`
	}{
		Subscribers: subscribers,
	}
	return q.queues(q.Name, "subscribers").Req("DELETE", &collection, nil)
}

func (q Queue) MessageSubscribersPollN(msgId string, n int) ([]Subscriber, error) {
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

func (q Queue) AddAlerts(alerts ...*Alert) (err error) {
	var queue struct {
		QI QueueInfo `json:"queue"`
	}
	in := QueueInfo{
		Alerts: make([]Alert, len(alerts)),
	}

	for i, alert := range alerts {
		in.Alerts[i] = *alert
	}
	queue.QI = in

	return q.queues(q.Name).Req("PATCH", &queue, nil)
}

func actualPushStatus(subs []Subscriber) bool {
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
func (m *Message) Touch() (err error) {
	return m.TouchFor(0)
}

// Reset timeout of message to keep it reserved
func (m *Message) TouchFor(timeout int) (err error) {
	reservationId, error := m.q.TouchMessageFor(m.Id, m.ReservationId, timeout)
	m.ReservationId = reservationId
	return error
}

// Put message back in the queue, message will be available after +delay+ seconds.
func (m Message) Release(delay int64) (err error) {
	return m.q.ReleaseMessage(m.Id, m.ReservationId, delay)
}

func (m Message) Subscribers() (interface{}, error) {
	return m.q.MessageSubscribers(m.Id)
}
