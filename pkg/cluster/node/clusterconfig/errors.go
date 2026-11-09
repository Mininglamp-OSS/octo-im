package clusterconfig

import "errors"

var (
	ErrEmptyReqs      = errors.New("empty reqs")
	ErrMembershipBusy = errors.New("configuration membership change in progress")
)
