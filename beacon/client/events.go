package client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/goccy/go-json"
)

const (
	EventStreamBufferSize = 5

	// time between event stream requests
	EventStreamTimeout = 4 * time.Second
)

type Event struct {
	Topic string
	Data  any

	Error error
}

type HeadEvent struct {
	Slot                      string `json:"slot"`
	Block                     string `json:"block"`
	State                     string `json:"state"`
	EpochTransition           bool   `json:"epoch_transition"`
	CurrentDutyDependentRoot  string `json:"current_duty_dependent_root,omitempty"`
	PreviousDutyDependentRoot string `json:"previous_duty_dependent_root,omitempty"`
}

func startEventStream(ctx context.Context, url string) (chan Event, context.CancelFunc) {
	// create child context to cancel the event stream
	eventStreamCtx, cancel := context.WithCancel(ctx)

	ch := make(chan Event, EventStreamBufferSize)
	
	client := &http.Client{}

	// start event listener
	go func() {
		var newEvent strings.Builder
		for {
			// Exit if the context has been cancelled.
			select {
			case <-eventStreamCtx.Done():
				close(ch)
				return
			case <-time.After(EventStreamTimeout):
			}
			
			req, err := http.NewRequestWithContext(eventStreamCtx, "GET", url, nil)
			if err != nil {
				ch <- Event{Error: errors.Join(errors.New("error creating event stream request"), err)}
				continue
			}
		
			req.Header.Set("Cache-Control", "no-cache")
			req.Header.Set("Accept", "text/event-stream")
			req.Header.Set("Connection", "keep-alive")


			resp, err := client.Do(req)
			if err != nil {
				ch <- Event{Error: errors.Join(errors.New("error reading event stream"), err)}
				continue
			}

			if resp.StatusCode != http.StatusOK {
				resp.Body.Close()
				ch <- Event{Error: fmt.Errorf("received unexpected status code: %s", resp.Status)}
				continue
			}


			scanner := bufio.NewScanner(resp.Body)
			for scanner.Scan() {
				// Exit if the context has been cancelled.
				select {
				case <-eventStreamCtx.Done():
					resp.Body.Close()
					
					// clear string building in case a partial msg is still stored
					newEvent.Reset()
					return
				default:
				}

				line := scanner.Text()

				if line == "" {
					if newEvent.Len() > 0 {
						ch <- parseEvent(newEvent.String())
						newEvent.Reset()
					}
					continue
				}

				newEvent.WriteString(line + "\n")
			}
			resp.Body.Close()

			if err := scanner.Err(); err != nil {
				ch <- Event{Error: errors.Join(errors.New("error reading event stream"), err)}
				continue
			}
		}
	}()

	return ch, cancel
}

func parseEvent(eventStr string) Event {
	parts := strings.Split(eventStr, "\n")
	if len(parts) < 2 {
		return Event{Error: fmt.Errorf("invalid event format: Number of parts is %d", len(parts))}
	}

	if !strings.HasPrefix(parts[0], "event: ") {
		return Event{Error: fmt.Errorf("invalid event format: Missing event type")}
	}

	if !strings.HasPrefix(parts[1], "data: ") {
		return Event{Error: fmt.Errorf("invalid event format: Missing event data")}
	}

	// remove prefix and sanitize data
	eventType := strings.TrimSpace(strings.TrimPrefix(parts[0], "event: "))
	data := strings.TrimSpace(strings.TrimPrefix(parts[1], "data: "))

	switch eventType {
	case "head":
		var headEventData HeadEvent
		if err := json.Unmarshal([]byte(data), &headEventData); err != nil {
			return Event{Error: fmt.Errorf("error decoding head event data: %s", err.Error())}
		}
		return Event{eventType, &headEventData, nil}
	default:
		return Event{Error: fmt.Errorf("unsupported event type: %s", eventType)}
	}
}
