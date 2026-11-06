package ingress

// UpdateTagForTest exercises the production RPC operation without a socket.
var UpdateTagForTest = (*Ingress).updateTag
