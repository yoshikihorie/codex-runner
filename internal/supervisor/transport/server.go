package transport

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/proc"
	corestore "github.com/yoshikihorie/codex-runner/internal/store"
)

// Supervisor socket constants are copied from specifications/10-shared/validation-rules.md §3.1.
const (
	SupervisorProtocolVersion      = 1
	SupervisorProtocolLineMaxBytes = 1048576
	SupervisorRequestReadTimeout   = 5 * time.Second
	SupervisorSocketConnectTimeout = 5 * time.Second // Canonical SOCKET_CONNECT_TIMEOUT_SECONDS.
)

type Request struct {
	SupervisorProtocolVersion int             `json:"supervisor_protocol_version"`
	Verb                      string          `json:"verb"`
	Params                    json.RawMessage `json:"params"`
}
type Response struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *ErrorBody      `json:"error,omitempty"`
}
type ErrorBody struct {
	Code       string         `json:"code"`
	MessageKey string         `json:"message_key"`
	Details    map[string]any `json:"details,omitempty"`
}

type Handler interface {
	Handle(context.Context, Request, int) Response
}

type Server struct {
	SocketPath        string
	DefaultSocketPath string
	LockHeld          bool
	Handler           Handler
	PeerCredentials   func(*net.UnixConn) (int, int, error)
	ReadTimeout       time.Duration
}

func malformedResponse() Response {
	return Response{Error: &ErrorBody{Code: "SUP_REQUEST_MALFORMED", MessageKey: "error.supervisor.requestMalformed"}}
}

func (s *Server) Serve(ctx context.Context) error {
	if s.Handler == nil {
		return errors.New("supervisor handler is required")
	}
	if !filepath.IsAbs(s.SocketPath) {
		return errors.New("socket path must be absolute")
	}
	parent := filepath.Dir(s.SocketPath)
	info, err := os.Lstat(parent)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0700 {
		return errors.New("socket parent must be a private real directory")
	}
	if existing, err := os.Lstat(s.SocketPath); err == nil {
		if !s.LockHeld || s.SocketPath != s.DefaultSocketPath || existing.Mode()&os.ModeSocket == 0 {
			return errors.New("SUP_CONFIG_PATH_CONFLICT")
		}
		if err := quarantineOwnedSocket(s.SocketPath, existing); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: s.SocketPath, Net: "unix"})
	if err != nil {
		return err
	}
	defer listener.Close()
	if err := os.Chmod(s.SocketPath, 0600); err != nil {
		return err
	}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-done:
		}
	}()
	for {
		conn, err := listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go s.serveConn(ctx, conn)
	}
}

func quarantineOwnedSocket(path string, expected os.FileInfo) error {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	quarantine := filepath.Join(filepath.Dir(path), fmt.Sprintf(".%s.quarantine-%x", filepath.Base(path), nonce))
	if err := os.Rename(path, quarantine); err != nil {
		return err
	}
	actual, err := os.Lstat(quarantine)
	if err == nil && actual.Mode()&os.ModeSocket != 0 && os.SameFile(expected, actual) {
		return os.Remove(quarantine)
	}
	if restoreErr := corestore.RenameExclusive(quarantine, path); restoreErr != nil {
		return fmt.Errorf("preserve quarantined socket: %w", restoreErr)
	}
	return errors.New("socket changed during quarantine")
}

func (s *Server) serveConn(ctx context.Context, conn *net.UnixConn) {
	defer conn.Close()
	peer := s.PeerCredentials
	if peer == nil {
		peer = proc.PeerCredentials
	}
	uid, pid, err := peer(conn)
	if err != nil || uid != os.Geteuid() {
		return
	} // SUP_PEER_REJECTED: no request read, no response.
	limit := s.ReadTimeout
	if limit <= 0 {
		limit = SupervisorRequestReadTimeout
	}
	_ = conn.SetReadDeadline(time.Now().Add(limit))
	line, err := bufio.NewReader(io.LimitReader(conn, SupervisorProtocolLineMaxBytes+2)).ReadBytes('\n')
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return
	}
	if err != nil || len(line) > SupervisorProtocolLineMaxBytes {
		writeResponse(conn, malformedResponse())
		return
	}
	var request Request
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || request.SupervisorProtocolVersion != SupervisorProtocolVersion {
		writeResponse(conn, malformedResponse())
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		writeResponse(conn, malformedResponse())
		return
	}
	response := s.Handler.Handle(ctx, request, pid)
	writeResponse(conn, response)
}

func writeResponse(conn *net.UnixConn, response Response) {
	data, err := json.Marshal(response)
	if err != nil || len(data)+1 > SupervisorProtocolLineMaxBytes {
		return
	}
	_, _ = io.Copy(conn, bytes.NewReader(append(data, '\n')))
}
