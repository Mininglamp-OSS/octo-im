package clusterconfig

import "errors"

var (
	ErrEmptyReqs = errors.New("empty reqs")
	// ErrServerLifecycle reports Start on a Server that was already started
	// or stopped. A Server is single-use; restart with a new Server.
	ErrServerLifecycle = errors.New("clusterconfig: server is single-use; construct a new Server to restart")
)
