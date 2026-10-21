package transport

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestClientRejectsOverlongUnterminatedResponse(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "sup-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	path := filepath.Join(root, "s.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, e := listener.Accept()
		if e != nil {
			return
		}
		defer conn.Close()
		buffer := make([]byte, 1024)
		_, _ = conn.Read(buffer)
		_, _ = conn.Write(bytes.Repeat([]byte("x"), SupervisorProtocolLineMaxBytes+1))
	}()
	_, err = (Client{SocketPath: path}).Call(context.Background(), Request{Verb: "status"})
	if !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("error=%v", err)
	}
}

func TestClientClassifiesResponseTimeout(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "sup-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	path := filepath.Join(root, "s.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, e := listener.Accept()
		if e == nil {
			defer conn.Close()
			buffer := make([]byte, 1024)
			_, _ = conn.Read(buffer)
			time.Sleep(100 * time.Millisecond)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = (Client{SocketPath: path}).Call(ctx, Request{Verb: "status"})
	if !errors.Is(err, ErrClientTimeout) {
		t.Fatalf("error=%v", err)
	}
}

func TestClient_SCNsup0528_DedicatedSocketUnavailable(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	client := Client{SocketPath: filepath.Join(home, "supervisor.sock")}
	_, err = client.Call(context.Background(), Request{Verb: "status"})
	if !errors.Is(err, ErrClientUnavailable) {
		t.Fatalf("error=%v", err)
	}
}

func TestClientResponseWaitOutlivesDialTimeout(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "sup-sock-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	listener, err := net.Listen("unix", filepath.Join(root, "s.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, e := listener.Accept()
		if e != nil {
			return
		}
		defer conn.Close()
		buffer := make([]byte, 1024)
		_, _ = conn.Read(buffer)
		time.Sleep(60 * time.Millisecond)
		_, _ = conn.Write([]byte("{\"ok\":true,\"result\":{}}\n"))
	}()
	response, err := (Client{SocketPath: filepath.Join(root, "s.sock"), DialTimeout: 20 * time.Millisecond}).Call(context.Background(), Request{Verb: "stop-session"})
	if err != nil || !response.OK {
		t.Fatalf("response=%+v err=%v", response, err)
	}
}

func TestClientClosedAndMalformedResponseDiffer(t *testing.T) {
	for _, tc := range []struct {
		name, reply string
		want        error
	}{{"closed", "", ErrClientClosed}, {"malformed", "not json\n", ErrInvalidResponse}} {
		t.Run(tc.name, func(t *testing.T) {
			root, err := os.MkdirTemp("/tmp", "sup-sock-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(root) })
			path := filepath.Join(root, "s.sock")
			listener, err := net.Listen("unix", path)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			go func() {
				conn, e := listener.Accept()
				if e != nil {
					return
				}
				defer conn.Close()
				buffer := make([]byte, 1024)
				_, _ = conn.Read(buffer)
				_, _ = conn.Write([]byte(tc.reply))
			}()
			_, err = (Client{SocketPath: path}).Call(context.Background(), Request{Verb: "status"})
			if !errors.Is(err, tc.want) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}
