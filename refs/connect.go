package refs

import (
	"context"

	"github.com/amber-store/core/packstore"
	"github.com/amber-store/jaccard-store/client"
	"github.com/amber-store/jaccard-store/node"
	"github.com/amber-store/jaccard-store/wire"
)

// Conn is a connection to a jaccard-store server, as far as the store needs
// one: references are pulled over it. Pull is called from many goroutines
// at once.
type Conn interface {
	Pull(ctx context.Context, objects *packstore.Store, name string, opts client.PullOptions) (client.PullResult, error)
	Close() error
}

// serverConn is jaccard-store's client over a connection that was dialed
// for it.
type serverConn struct {
	conn   *node.Conn
	client *client.Client
}

// Connect dials the server of cfg for jaccard-store's protocol, whatever
// ALPN cfg names. It is what Options.Dial calls outside of tests. ctx
// bounds the dial alone; the connection lasts until it is closed.
func Connect(ctx context.Context, cfg node.DialConfig) (Conn, error) {
	cfg.ALPN = wire.ALPN
	conn, err := node.Dial(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &serverConn{conn: conn, client: client.New(conn, nil)}, nil
}

func (c *serverConn) Pull(ctx context.Context, objects *packstore.Store, name string, opts client.PullOptions) (client.PullResult, error) {
	return c.client.Pull(ctx, objects, name, opts)
}

// Close closes the connection and the endpoint that was bound for it.
func (c *serverConn) Close() error {
	return c.conn.Close()
}
