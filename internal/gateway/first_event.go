package gateway

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	wire "opencode2api/internal/protocol"
)

var errFirstEventTimeout = fmt.Errorf("upstream first assistant delta timed out: %w", context.DeadlineExceeded)

// defaultFirstContentTimeout bounds how long an attempt may wait for the first
// real assistant delta before the retry loop abandons it. It applies when the
// config leaves performance.first_event_timeout_seconds at zero.
const defaultFirstContentTimeout = 90 * time.Second

func firstContentTimeout(configuredSeconds int) time.Duration {
	if configuredSeconds > 0 {
		return time.Duration(configuredSeconds) * time.Second
	}
	return defaultFirstContentTimeout
}

type attemptBody struct {
	io.Reader
	body   io.ReadCloser
	cancel context.CancelCauseFunc
}

func (b *attemptBody) Close() error {
	b.cancel(nil)
	return b.body.Close()
}

// Wait before returning a successful SSE response to the retry loop, while
// nothing has been committed downstream. Keep every byte for native forwarding.
// Only a real assistant delta (content, reasoning, or a tool call) counts:
// role-only deltas, lifecycle events, comments, and incomplete frames are
// ignored. A stream that ends or errors first is a failed attempt, so the
// caller can retry the next lane.
func doInferenceAttempt(client *http.Client, req *http.Request, timeout time.Duration, protocol wire.Protocol) (*http.Response, error) {
	if strings.HasSuffix(req.URL.Path, "/systemone") {
		return client.Do(req)
	}
	if timeout <= 0 {
		timeout = defaultFirstContentTimeout
	}
	ctx, cancel := context.WithCancelCause(req.Context())
	resp, err := client.Do(req.Clone(ctx))
	if err != nil {
		cancel(nil)
		return resp, err
	}
	if resp.StatusCode/100 != 2 || !eventStreamResponse(resp) {
		resp.Body = &attemptBody{Reader: resp.Body, body: resp.Body, cancel: cancel}
		return resp, nil
	}
	timer := time.AfterFunc(timeout, func() { cancel(errFirstEventTimeout) })
	reader := bufio.NewReader(resp.Body)
	prefix, err := readFirstRealEvent(reader, protocol)
	stopped := timer.Stop()
	if !stopped && ctx.Err() == nil {
		// Stop(false) means the callback may still be about to run.
		cancel(errFirstEventTimeout)
	}
	if cause := context.Cause(ctx); cause != nil {
		err = cause
	}
	if err != nil {
		cancel(nil)
		resp.Body.Close()
		return nil, err
	}
	resp.Body = &attemptBody{Reader: io.MultiReader(bytes.NewReader(prefix), reader), body: resp.Body, cancel: cancel}
	return resp, nil
}

// readFirstRealEvent consumes complete SSE frames until one carries a real
// assistant delta, returning every byte read so the response can still be
// forwarded unchanged. Comments and partial frames are not events.
func readFirstRealEvent(reader *bufio.Reader, protocol wire.Protocol) ([]byte, error) {
	const maxPrelude = 4 << 20
	var prefix []byte
	var line []byte
	var eventName string
	var dataLines []string
	for {
		part, err := reader.ReadSlice('\n')
		if len(prefix)+len(part) > maxPrelude {
			return nil, errors.New("upstream SSE prelude exceeds 4 MiB")
		}
		prefix = append(prefix, part...)
		line = append(line, part...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
		text := strings.TrimSuffix(strings.TrimSuffix(string(line), "\n"), "\r")
		line = line[:0]
		if text == "" {
			if len(dataLines) > 0 {
				data := strings.Join(dataLines, "\n")
				if data == "[DONE]" {
					return nil, fmt.Errorf("%w: stream terminated before any content", errUpstreamNoAssistantContent)
				}
				if message, isError := wire.StreamEventError(protocol, eventName, data); isError {
					return nil, fmt.Errorf("%w: %s", errUpstreamInvalidContent, message)
				}
				if wire.IsAssistantDelta(protocol, eventName, data) {
					return prefix, nil
				}
			}
			eventName = ""
			dataLines = dataLines[:0]
			continue
		}
		if strings.HasPrefix(text, ":") {
			continue
		}
		field, value, found := strings.Cut(text, ":")
		if !found {
			field, value = text, ""
		}
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			eventName = strings.TrimSpace(value)
		case "data":
			dataLines = append(dataLines, value)
		}
	}
}
