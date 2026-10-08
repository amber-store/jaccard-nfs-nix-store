//go:build linux

package nfsd

import (
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual/nfsv4"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/random"
	nfsv4_xdr "github.com/buildbarn/go-xdr/pkg/protocols/nfsv4"
	"github.com/buildbarn/go-xdr/pkg/protocols/rpcv2"
	"github.com/buildbarn/go-xdr/pkg/rpcserver"
)

const (
	// readSize is the largest read announced to clients, and what the
	// Linux client then reads at a time.
	readSize = 1 << 20
	// A client that has said nothing for enforcedLease loses its state.
	// It is told announcedLease, which leaves room for a slow network.
	// The two are what Buildbarn recommends.
	enforcedLease  = 120 * time.Second
	announcedLease = 60 * time.Second
	// An accept that failed is tried again after acceptWait, and after
	// twice as long each further time, up to acceptWaitMax.
	acceptWait    = 5 * time.Millisecond
	acceptWaitMax = time.Second
)

// Server serves a file system over NFSv4.1 and NFSv4.0.
type Server struct {
	rpc *rpcserver.Server
	log *slog.Logger

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
	served sync.WaitGroup
}

// NewServer returns a server of fs, set up as Buildbarn sets its own up:
// both minor versions behind one program, no authentication, and
// identifiers drawn anew, by which a client tells that the server was
// restarted and takes up with it again.
func NewServer(fs *FS) *Server {
	var soMajorID [8]byte
	random.FastThreadSafeGenerator.Read(soMajorID[:])
	serverOwner := nfsv4_xdr.ServerOwner4{
		SoMinorId: random.FastThreadSafeGenerator.Uint64(),
		SoMajorId: soMajorID[:],
	}
	var serverScope [8]byte
	random.FastThreadSafeGenerator.Read(serverScope[:])
	var rebootVerifier nfsv4_xdr.Verifier4
	random.FastThreadSafeGenerator.Read(rebootVerifier[:])
	var stateIDOtherPrefix [4]byte
	random.FastThreadSafeGenerator.Read(stateIDOtherPrefix[:])

	rootDirectory := fs.Root()
	openedFiles := nfsv4.NewOpenedFilesPool(fs.Resolve)
	security := []nfsv4_xdr.Secinfo4{&nfsv4_xdr.Secinfo4_default{Flavor: rpcv2.AUTH_NONE}}
	program := nfsv4.NewMinorVersionFallbackProgram([]nfsv4_xdr.Nfs4Program{
		nfsv4.NewNFS41Program(
			rootDirectory,
			openedFiles,
			serverOwner,
			serverScope[:],
			&nfsv4_xdr.ChannelAttrs4{
				// Room for a read of readSize and what goes around it.
				CaMaxrequestsize:        2 * readSize,
				CaMaxresponsesize:       2 * readSize,
				CaMaxresponsesizeCached: 64 * 1024,
				CaMaxoperations:         1000,
				CaMaxrequests:           100,
			},
			random.NewFastSingleThreadedGenerator(),
			rebootVerifier,
			clock.SystemClock,
			enforcedLease,
			announcedLease,
			path.LocalFormat,
			security,
		),
		nfsv4.NewNFS40Program(
			rootDirectory,
			openedFiles,
			random.NewFastSingleThreadedGenerator(),
			rebootVerifier,
			stateIDOtherPrefix,
			clock.SystemClock,
			enforcedLease,
			announcedLease,
			path.LocalFormat,
			security,
		),
	})
	return &Server{
		rpc: rpcserver.NewServer(map[uint32]rpcserver.Service{
			nfsv4_xdr.NFS4_PROGRAM_PROGRAM_NUMBER: nfsv4_xdr.NewNfs4ProgramService(
				&maxReadProgram{inner: program, max: readSize},
			),
		}, rpcserver.AllowAuthenticator),
		log:   fs.log,
		conns: map[net.Conn]struct{}{},
	}
}

// Serve accepts connections on l and serves each until it ends. It
// returns when l is closed or the server is.
//
// An accept that fails for another reason is tried again, after 5 ms and
// then twice as long each time up to a second: the process may be out of
// file descriptors for a moment, or a client gone before it was accepted,
// and a server that stopped accepting then would leave the mount without
// one for as long as the process lives.
func (s *Server) Serve(l net.Listener) error {
	var wait time.Duration
	for {
		c, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || s.isClosed() {
				return nil
			}
			wait = min(max(2*wait, acceptWait), acceptWaitMax)
			s.log.Warn("accepting an NFS connection failed, and is tried again", "in", wait, "error", err)
			time.Sleep(wait)
			continue
		}
		wait = 0
		if !s.add(c) {
			c.Close()
			return nil
		}
		go func() {
			defer s.remove(c)
			if err := s.rpc.HandleConnection(c, c); err != nil {
				s.log.Debug("an NFS connection ended", "client", c.RemoteAddr().String(), "error", err)
			}
		}()
	}
}

func (s *Server) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Server) add(c net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.conns[c] = struct{}{}
	s.served.Add(1)
	return true
}

func (s *Server) remove(c net.Conn) {
	c.Close()
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
	s.served.Done()
}

// Close closes every connection and waits for their serving to end. The
// listener is the caller's to close. Closing twice is no error.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.served.Wait()
	return nil
}
