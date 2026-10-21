package transport

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor/usecase"
)

var ErrClientUnavailable = errors.New("SUP_CLIENT_UNAVAILABLE")
var ErrInvalidResponse = errors.New("SUP_REQUEST_MALFORMED")
var ErrClientTimeout = errors.New("supervisor response timeout")
var ErrClientClosed = errors.New("supervisor connection closed")

// Response limit = SOCKET_CONNECT_TIMEOUT_SECONDS + SUPERVISOR_STOP_SESSION_EXIT_CONFIRM_SECONDS.
var supervisorResponseWaitTimeout = SupervisorSocketConnectTimeout + usecase.StopSessionExitConfirmTimeout()

type Client struct {
	SocketPath  string
	DialTimeout time.Duration
}

func (c Client) Call(ctx context.Context, request Request) (Response, error) {
	if request.SupervisorProtocolVersion == 0 {
		request.SupervisorProtocolVersion = SupervisorProtocolVersion
	}
	data, err := json.Marshal(request)
	if err != nil || len(data)+1 > SupervisorProtocolLineMaxBytes {
		return Response{}, errors.New("SUP_REQUEST_MALFORMED")
	}
	timeout := c.DialTimeout
	if timeout <= 0 {
		timeout = SupervisorSocketConnectTimeout
	}
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "unix", c.SocketPath)
	if err != nil {
		return Response{}, errors.Join(ErrClientUnavailable, err)
	}
	defer conn.Close()
	deadline := time.Now().Add(timeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = conn.SetDeadline(deadline)
	if _, err := io.Copy(conn, bytes.NewReader(append(data, '\n'))); err != nil {
		return Response{}, errors.Join(ErrClientUnavailable, err)
	}
	deadline = time.Now().Add(supervisorResponseWaitTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = conn.SetReadDeadline(deadline)
	line, err := bufio.NewReader(io.LimitReader(conn, SupervisorProtocolLineMaxBytes+1)).ReadBytes('\n')
	if len(line) > SupervisorProtocolLineMaxBytes {
		return Response{}, ErrInvalidResponse
	}
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return Response{}, errors.Join(ErrClientUnavailable, ErrClientTimeout, err)
		}
		if errors.Is(err, io.EOF) {
			return Response{}, errors.Join(ErrClientUnavailable, ErrClientClosed, err)
		}
		return Response{}, errors.Join(ErrClientUnavailable, err)
	}
	if len(line) > SupervisorProtocolLineMaxBytes {
		return Response{}, ErrInvalidResponse
	}
	var response Response
	if err := json.Unmarshal(line, &response); err != nil {
		return Response{}, errors.Join(ErrInvalidResponse, err)
	}
	if response.OK == (response.Error != nil) || response.OK && len(response.Result) == 0 {
		return Response{}, ErrInvalidResponse
	}
	return response, nil
}
