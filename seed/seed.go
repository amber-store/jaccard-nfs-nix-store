// Package seed names the dependencies of the packages that are still to be
// written, so that go.mod and go.sum hold them from the start. It is removed
// when they are there.
package seed

import (
	_ "github.com/amber-store/core/fstree"
	_ "github.com/amber-store/core/ingest"
	_ "github.com/amber-store/core/key"
	_ "github.com/amber-store/core/packstore"
	_ "github.com/amber-store/core/reference"
	_ "github.com/amber-store/jaccard-store/bucket"
	_ "github.com/amber-store/jaccard-store/bucket/buckettest"
	_ "github.com/amber-store/jaccard-store/client"
	_ "github.com/amber-store/jaccard-store/db"
	_ "github.com/amber-store/jaccard-store/node"
	_ "github.com/amber-store/jaccard-store/server"
	_ "github.com/amber-store/jaccard-store/wire"
	_ "github.com/tmc/go-iroh/iroh"
	_ "github.com/tmc/go-iroh/key"
	_ "github.com/urfave/cli/v2"
	_ "github.com/zeebo/blake3"
	_ "golang.org/x/sys/unix"
)
