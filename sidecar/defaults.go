package sidecar

const (
	// DefaultListen is where the NFS server listens unless it is told
	// otherwise: the port of NFS, on loopback, because whoever can reach
	// the server can read the store.
	DefaultListen = "127.0.0.1:2049"

	// DefaultMountOptions are the options of the mount after the
	// server's address and port.
	//
	// vers=4.1 because the server speaks nothing older than NFSv4.
	// soft with timeo=600 and retrans=2 so that a request the server
	// does not answer fails after three minutes instead of hanging for
	// ever, which also bounds how long the end of a pod is held up by a
	// mount whose server is gone. lookupcache=positive so that a name
	// that was missing is asked for again: a reference may be pushed at
	// any time.
	DefaultMountOptions = "vers=4.1,soft,timeo=600,retrans=2,lookupcache=positive"
)
