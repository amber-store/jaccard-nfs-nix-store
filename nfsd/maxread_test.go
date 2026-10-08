//go:build linux

package nfsd

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"testing"

	nfsv4_xdr "github.com/buildbarn/go-xdr/pkg/protocols/nfsv4"
)

const (
	bitSupported  = 1 << nfsv4_xdr.FATTR4_SUPPORTED_ATTRS
	bitType       = 1 << nfsv4_xdr.FATTR4_TYPE
	bitLease      = 1 << nfsv4_xdr.FATTR4_LEASE_TIME
	bitACL        = 1 << nfsv4_xdr.FATTR4_ACL
	bitFilehandle = 1 << nfsv4_xdr.FATTR4_FILEHANDLE
	bitFileid     = 1 << nfsv4_xdr.FATTR4_FILEID
	bitMaxRead    = 1 << nfsv4_xdr.FATTR4_MAXREAD
	bitMaxWrite   = 1 << nfsv4_xdr.FATTR4_MAXWRITE
	bitMode       = 1 << (nfsv4_xdr.FATTR4_MODE - 32)
)

func u32(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }
func u64(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }

// opaque is a variable-length value as XDR writes it: its length, its
// bytes, and zeros up to a multiple of four.
func opaque(b []byte) []byte {
	out := append(u32(uint32(len(b))), b...)
	for len(out)%4 != 0 {
		out = append(out, 0)
	}
	return out
}

func join(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

const announced = 1 << 20

func TestTheSupportedAttributesNameTheTwo(t *testing.T) {
	// The list of supported attributes is itself a bitmap of two words.
	f := nfsv4_xdr.Fattr4{
		Attrmask: []uint32{bitSupported | bitType},
		AttrVals: join(u32(2), u32(bitSupported|bitType|bitLease), u32(bitMode), u32(2)),
	}
	announce(&f, []uint32{bitSupported | bitType}, announced)

	want := join(u32(2), u32(bitSupported|bitType|bitLease|bitMaxRead|bitMaxWrite), u32(bitMode), u32(2))
	if !bytes.Equal(f.AttrVals, want) {
		t.Fatalf("values %x, want %x", f.AttrVals, want)
	}
	if !slices.Equal(f.Attrmask, []uint32{bitSupported | bitType}) {
		t.Fatalf("mask %x: the two were not asked for, and are in the answer", f.Attrmask)
	}
}

func TestTheTwoAreGivenWhenAskedFor(t *testing.T) {
	// What the Linux client asks when it mounts: the limits and the lease.
	// The program knows only the lease.
	f := nfsv4_xdr.Fattr4{Attrmask: []uint32{bitLease}, AttrVals: u32(60)}
	announce(&f, []uint32{bitLease | bitMaxRead | bitMaxWrite}, announced)

	if want := join(u32(60), u64(announced), u64(announced)); !bytes.Equal(f.AttrVals, want) {
		t.Fatalf("values %x, want %x", f.AttrVals, want)
	}
	if want := []uint32{bitLease | bitMaxRead | bitMaxWrite}; !slices.Equal(f.Attrmask, want) {
		t.Fatalf("mask %x, want %x", f.Attrmask, want)
	}
}

func TestOnlyWhatWasAskedForIsGiven(t *testing.T) {
	f := nfsv4_xdr.Fattr4{Attrmask: []uint32{bitLease}, AttrVals: u32(60)}
	announce(&f, []uint32{bitLease | bitMaxRead}, announced)

	if want := join(u32(60), u64(announced)); !bytes.Equal(f.AttrVals, want) {
		t.Fatalf("values %x, want %x", f.AttrVals, want)
	}
	if want := []uint32{bitLease | bitMaxRead}; !slices.Equal(f.Attrmask, want) {
		t.Fatalf("mask %x, want %x", f.Attrmask, want)
	}
}

func TestTheTwoGoBetweenTheWords(t *testing.T) {
	// Values are in the order of their numbers: the two come after every
	// value of the first word the program writes, a file handle of a
	// length that is no multiple of four among them, and before the
	// values of the second word.
	handle := []byte{1, 2, 3, 4, 5, 6}
	first := join(u32(1), opaque(handle), u64(42))
	second := u32(0o555)
	f := nfsv4_xdr.Fattr4{
		Attrmask: []uint32{bitType | bitFilehandle | bitFileid, bitMode},
		AttrVals: join(first, second),
	}
	announce(&f, []uint32{bitType | bitFilehandle | bitFileid | bitMaxRead | bitMaxWrite, bitMode}, announced)

	if want := join(first, u64(announced), u64(announced), second); !bytes.Equal(f.AttrVals, want) {
		t.Fatalf("values %x, want %x", f.AttrVals, want)
	}
	if want := []uint32{bitType | bitFilehandle | bitFileid | bitMaxRead | bitMaxWrite, bitMode}; !slices.Equal(f.Attrmask, want) {
		t.Fatalf("mask %x, want %x", f.Attrmask, want)
	}
}

func TestAnAnswerWithNoAttributesAtAll(t *testing.T) {
	f := nfsv4_xdr.Fattr4{}
	announce(&f, []uint32{bitMaxRead}, announced)
	if want := u64(announced); !bytes.Equal(f.AttrVals, want) {
		t.Fatalf("values %x, want %x", f.AttrVals, want)
	}
	if want := []uint32{bitMaxRead}; !slices.Equal(f.Attrmask, want) {
		t.Fatalf("mask %x, want %x", f.Attrmask, want)
	}
}

func TestAnAnswerIsLeftAlone(t *testing.T) {
	for _, c := range []struct {
		name    string
		mask    []uint32
		vals    []byte
		request []uint32
	}{
		{"when the two are not asked for", []uint32{bitType}, u32(2), []uint32{bitType}},
		{"when nothing is asked for", []uint32{bitType}, u32(2), nil},
		// The program writes no ACL. If it ever does, its length is not
		// known here, and neither is where the first word's values end.
		{"with an attribute of unknown size", []uint32{bitType | bitACL}, join(u32(2), u32(0)), []uint32{bitType | bitACL | bitMaxRead}},
		{"with a file handle cut short", []uint32{bitFilehandle}, []byte{0, 0}, []uint32{bitFilehandle | bitMaxRead}},
		{"with a file handle longer than the values", []uint32{bitFilehandle}, join(u32(64), []byte{1, 2}), []uint32{bitFilehandle | bitMaxRead}},
		{"with values shorter than the mask says", []uint32{bitType | bitFileid}, u32(2), []uint32{bitType | bitFileid | bitMaxRead}},
		{"when it has the two already", []uint32{bitMaxRead | bitMaxWrite}, join(u64(7), u64(7)), []uint32{bitMaxRead | bitMaxWrite}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := nfsv4_xdr.Fattr4{Attrmask: slices.Clone(c.mask), AttrVals: slices.Clone(c.vals)}
			announce(&f, c.request, announced)
			if !bytes.Equal(f.AttrVals, c.vals) || !slices.Equal(f.Attrmask, c.mask) {
				t.Fatalf("mask %x values %x, were %x and %x", f.Attrmask, f.AttrVals, c.mask, c.vals)
			}
		})
	}
}

// program answers a compound with what a test prepared.
type program struct {
	res *nfsv4_xdr.Compound4res
	err error
}

func (p program) NfsV4Nfsproc4Null(context.Context) error { return p.err }
func (p program) NfsV4Nfsproc4Compound(context.Context, *nfsv4_xdr.Compound4args) (*nfsv4_xdr.Compound4res, error) {
	return p.res, p.err
}

func getattrOK(mask []uint32, vals []byte) *nfsv4_xdr.NfsResop4_OP_GETATTR {
	return &nfsv4_xdr.NfsResop4_OP_GETATTR{Opgetattr: &nfsv4_xdr.Getattr4res_NFS4_OK{
		Resok4: nfsv4_xdr.Getattr4resok{ObjAttributes: nfsv4_xdr.Fattr4{Attrmask: mask, AttrVals: vals}},
	}}
}

func valsOf(t *testing.T, op nfsv4_xdr.NfsResop4) []byte {
	t.Helper()
	return op.(*nfsv4_xdr.NfsResop4_OP_GETATTR).Opgetattr.(*nfsv4_xdr.Getattr4res_NFS4_OK).Resok4.ObjAttributes.AttrVals
}

func TestEveryGetattrOfACompoundIsSeenTo(t *testing.T) {
	ask := func(mask ...uint32) *nfsv4_xdr.NfsArgop4_OP_GETATTR {
		return &nfsv4_xdr.NfsArgop4_OP_GETATTR{Opgetattr: nfsv4_xdr.Getattr4args{AttrRequest: mask}}
	}
	args := &nfsv4_xdr.Compound4args{Argarray: []nfsv4_xdr.NfsArgop4{
		&nfsv4_xdr.NfsArgop4_OP_READLINK{},
		ask(bitLease | bitMaxRead | bitMaxWrite),
		ask(bitLease),
		ask(bitLease | bitMaxRead),
		// The compound ended before this one: it has no answer.
		ask(bitMaxRead),
	}}
	refused := &nfsv4_xdr.NfsResop4_OP_GETATTR{Opgetattr: &nfsv4_xdr.Getattr4res_default{Status: nfsv4_xdr.NFS4ERR_STALE}}
	inner := program{res: &nfsv4_xdr.Compound4res{Resarray: []nfsv4_xdr.NfsResop4{
		&nfsv4_xdr.NfsResop4_OP_READLINK{},
		getattrOK([]uint32{bitLease}, u32(60)),
		getattrOK([]uint32{bitLease}, u32(60)),
		refused,
	}}}

	res, err := (&maxReadProgram{inner: inner, max: announced}).NfsV4Nfsproc4Compound(context.Background(), args)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := valsOf(t, res.Resarray[1]), join(u32(60), u64(announced), u64(announced)); !bytes.Equal(got, want) {
		t.Errorf("the getattr that asked: values %x, want %x", got, want)
	}
	if got, want := valsOf(t, res.Resarray[2]), u32(60); !bytes.Equal(got, want) {
		t.Errorf("the getattr that did not ask: values %x, want %x", got, want)
	}
	if res.Resarray[3] != refused {
		t.Error("a getattr that was refused was replaced")
	}
}

func TestWhatTheProgramFailsWithIsPassedOn(t *testing.T) {
	failure := errors.New("the program failed")
	wrapped := &maxReadProgram{inner: program{err: failure}, max: announced}
	if _, err := wrapped.NfsV4Nfsproc4Compound(context.Background(), &nfsv4_xdr.Compound4args{}); err != failure {
		t.Errorf("compound: %v", err)
	}
	if err := wrapped.NfsV4Nfsproc4Null(context.Background()); err != failure {
		t.Errorf("null: %v", err)
	}
}
