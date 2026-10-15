package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	supervisortransport "github.com/yoshikihorie/codex-runner/internal/supervisor/transport"
)

var supervisorClientCall = func(ctx context.Context, client supervisortransport.Client, request supervisortransport.Request) (supervisortransport.Response, error) {
	return client.Call(ctx, request)
}
var supervisorClientIsTerminal = supervisorStdinIsTerminal

func runSupervisorClient(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return supervisorClientUsage(stderr)
	}
	verb := args[0]
	switch verb {
	case "status", "stop", "resume", "start-session", "stop-session", "events", "screen":
	default:
		return supervisorClientUsage(stderr)
	}
	flags := flag.NewFlagSet(verb, flag.ContinueOnError)
	flags.SetOutput(stderr)
	socketPath := flags.String("socket-path", "", "")
	session := flags.String("session", "", "")
	project := flags.String("project", "", "")
	since := flags.Uint64("since-seq", 0, "")
	limit := flags.Int("limit", 0, "")
	if flags.Parse(args[1:]) != nil || len(flags.Args()) != 0 {
		return 2
	}
	seen := specifiedFlags(flags)
	if seen["session"] {
		if _, err := supervisor.NewSessionID(*session); err != nil {
			return supervisorClientUsage(stderr)
		}
	}
	if ((verb == "stop-session" || verb == "screen") && !seen["session"]) || (verb == "start-session" && *project == "") || (verb != "start-session" && seen["project"]) || (verb != "events" && (seen["since-seq"] || seen["limit"])) || (verb == "events" && seen["limit"] && *limit < 1) || (verb == "start-session" && seen["session"]) {
		return supervisorClientUsage(stderr)
	}
	if *socketPath == "" {
		if seen["socket-path"] {
			return supervisorClientUsage(stderr)
		}
		home, err := os.UserHomeDir()
		if err != nil || home == "" {
			return supervisorClientUsage(stderr)
		}
		*socketPath = filepath.Join(home, ".claude", "run", "codexd-supervisor.sock")
	}
	if !filepath.IsAbs(*socketPath) {
		return supervisorClientUsage(stderr)
	}
	if verb == "resume" {
		file, ok := stdin.(*os.File)
		terminal := ok && supervisorClientIsTerminal(file)
		_, supervised := os.LookupEnv("CODEXD_SUPERVISED")
		if !terminal || supervised {
			writeSupervisorClientError(stdout, "SUP_RESUME_FORBIDDEN", "error.supervisor.resumeForbidden")
			return 1
		}
	}
	params := map[string]any{}
	if seen["session"] {
		params["session_id"] = *session
	}
	if verb == "start-session" {
		params["project_id"] = *project
	}
	if seen["since-seq"] {
		params["since_seq"] = *since
	}
	if seen["limit"] {
		params["limit"] = *limit
	}
	body, err := json.Marshal(params)
	if err != nil {
		return supervisorClientUsage(stderr)
	}
	request := supervisortransport.Request{SupervisorProtocolVersion: supervisortransport.SupervisorProtocolVersion, Verb: verb, Params: body}
	response, err := supervisorClientCall(ctx, supervisortransport.Client{SocketPath: *socketPath}, request)
	if err != nil {
		if errors.Is(err, supervisortransport.ErrClientUnavailable) {
			writeSupervisorClientError(stdout, "SUP_CLIENT_UNAVAILABLE", "error.supervisor.clientUnavailable")
			_, _ = fmt.Fprintln(stderr, "SUP_CLIENT_UNAVAILABLE error.supervisor.clientUnavailable")
			return 3
		}
		writeSupervisorClientError(stdout, "SUP_REQUEST_MALFORMED", "error.supervisor.requestMalformed")
		return 1
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return 1
	}
	_, _ = fmt.Fprintln(stdout, string(encoded))
	if !response.OK {
		return 1
	}
	return 0
}

func supervisorClientUsage(stderr io.Writer) int {
	_, _ = fmt.Fprintln(stderr, "SUP_REQUEST_MALFORMED error.supervisor.requestMalformed")
	return 2
}

func writeSupervisorClientError(stdout io.Writer, code, key string) {
	encoded, _ := json.Marshal(supervisortransport.Response{Error: &supervisortransport.ErrorBody{Code: code, MessageKey: key}})
	_, _ = fmt.Fprintln(stdout, string(encoded))
}
