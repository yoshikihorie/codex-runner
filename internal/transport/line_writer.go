package transport

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

var errProtocolLineTooLong = errors.New("protocol line exceeds maximum length")

func writeResponseLine(out io.Writer, response Response) error {
	encoded, err := json.Marshal(response)
	if err != nil {
		return err
	}
	if len(encoded) > protocolLineMaxBytes {
		return errProtocolLineTooLong
	}
	frame := append(encoded, '\n')
	return writeProtocolFrame(out, frame)
}

type protocolLineWriter struct {
	out     io.Writer
	pending []byte
	err     error
}

func newProtocolLineWriter(out io.Writer) io.Writer {
	return &protocolLineWriter{out: out}
}

func (w *protocolLineWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	written := 0
	for len(p) > 0 {
		newline := bytes.IndexByte(p, '\n')
		segment := p
		complete := newline >= 0
		if complete {
			segment = p[:newline]
		}
		if len(w.pending)+len(segment) > protocolLineMaxBytes {
			w.err = errProtocolLineTooLong
			return written, w.err
		}
		w.pending = append(w.pending, segment...)
		written += len(segment)
		p = p[len(segment):]
		if !complete {
			break
		}

		frame := make([]byte, len(w.pending)+1)
		copy(frame, w.pending)
		frame[len(w.pending)] = '\n'
		if err := writeProtocolFrame(w.out, frame); err != nil {
			w.err = err
			return written, err
		}
		w.pending = w.pending[:0]
		p = p[1:]
		written++
	}
	return written, nil
}

func writeProtocolFrame(out io.Writer, frame []byte) error {
	written, err := out.Write(frame)
	if written != len(frame) {
		if err == nil {
			return io.ErrShortWrite
		}
		return errors.Join(err, io.ErrShortWrite)
	}
	return err
}
