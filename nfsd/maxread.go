//go:build linux

package nfsd

import (
	"context"
	"encoding/binary"

	nfsv4_xdr "github.com/buildbarn/go-xdr/pkg/protocols/nfsv4"
)

// maxReadProgram adds FATTR4_MAXREAD and FATTR4_MAXWRITE to the answers of
// an NFSv4 program that does not know them.
//
// Buildbarn's programs do not, and the Linux client, told no maximum, reads
// 1024 bytes at a time. The client asks for the two only if the list of
// supported attributes names them, so they are added there as well as given
// when they are asked for.
type maxReadProgram struct {
	inner nfsv4_xdr.Nfs4Program
	// max is the size announced for both.
	max uint64
}

func (p *maxReadProgram) NfsV4Nfsproc4Null(ctx context.Context) error {
	return p.inner.NfsV4Nfsproc4Null(ctx)
}

func (p *maxReadProgram) NfsV4Nfsproc4Compound(ctx context.Context, args *nfsv4_xdr.Compound4args) (*nfsv4_xdr.Compound4res, error) {
	res, err := p.inner.NfsV4Nfsproc4Compound(ctx, args)
	if err != nil || res == nil {
		return res, err
	}
	// The answers are in the order of the operations, and end where the
	// compound did.
	for i, op := range res.Resarray {
		if i >= len(args.Argarray) {
			break
		}
		got, ok := op.(*nfsv4_xdr.NfsResop4_OP_GETATTR)
		if !ok {
			continue
		}
		asked, ok := args.Argarray[i].(*nfsv4_xdr.NfsArgop4_OP_GETATTR)
		if !ok {
			continue
		}
		if fine, ok := got.Opgetattr.(*nfsv4_xdr.Getattr4res_NFS4_OK); ok {
			announce(&fine.Resok4.ObjAttributes, asked.Opgetattr.AttrRequest, p.max)
		}
	}
	return res, nil
}

// theTwo are the bits of FATTR4_MAXREAD and FATTR4_MAXWRITE in the first
// word of an attribute bitmap.
const theTwo = 1<<nfsv4_xdr.FATTR4_MAXREAD | 1<<nfsv4_xdr.FATTR4_MAXWRITE

// word0Size is the encoded size of each attribute of the first bitmap word
// that Buildbarn writes and that has one size. The list of supported
// attributes and the file handle are measured where they stand.
var word0Size = map[uint]int{
	nfsv4_xdr.FATTR4_TYPE:            4,
	nfsv4_xdr.FATTR4_FH_EXPIRE_TYPE:  4,
	nfsv4_xdr.FATTR4_CHANGE:          8,
	nfsv4_xdr.FATTR4_SIZE:            8,
	nfsv4_xdr.FATTR4_LINK_SUPPORT:    4,
	nfsv4_xdr.FATTR4_SYMLINK_SUPPORT: 4,
	nfsv4_xdr.FATTR4_NAMED_ATTR:      4,
	nfsv4_xdr.FATTR4_FSID:            16,
	nfsv4_xdr.FATTR4_UNIQUE_HANDLES:  4,
	nfsv4_xdr.FATTR4_LEASE_TIME:      4,
	nfsv4_xdr.FATTR4_RDATTR_ERROR:    4,
	nfsv4_xdr.FATTR4_FILEID:          8,
}

// announce names the two among the supported attributes, where the answer f
// lists those, and gives each of the two that request asks for and f does
// not have.
//
// The values of an answer stand in the order of their attributes' numbers.
// The two have the highest numbers of the first word, so they belong where
// the first word's values end. To find that place the size of every value
// before it has to be known: an answer with an attribute this does not know
// the size of, or whose values end early, is left as it is.
func announce(f *nfsv4_xdr.Fattr4, request []uint32, max uint64) {
	var have uint32
	if len(f.Attrmask) > 0 {
		have = f.Attrmask[0]
	}
	end, ok := word0End(have&^theTwo, f.AttrVals)
	if !ok {
		return
	}
	// The list of supported attributes has the lowest number, so it is the
	// first value: a count of words and the words.
	if have&(1<<nfsv4_xdr.FATTR4_SUPPORTED_ATTRS) != 0 && binary.BigEndian.Uint32(f.AttrVals) >= 1 {
		binary.BigEndian.PutUint32(f.AttrVals[4:], binary.BigEndian.Uint32(f.AttrVals[4:])|theTwo)
	}

	var want uint32
	if len(request) > 0 {
		want = request[0] & theTwo &^ have
	}
	if want == 0 {
		return
	}
	if have&theTwo != 0 {
		// One of the two is there and the other is asked for. The
		// program does not do that; where the other would go depends on
		// which, and is not worth knowing.
		return
	}
	vals := make([]byte, 0, len(f.AttrVals)+16)
	vals = append(vals, f.AttrVals[:end]...)
	for _, bit := range []uint32{1 << nfsv4_xdr.FATTR4_MAXREAD, 1 << nfsv4_xdr.FATTR4_MAXWRITE} {
		if want&bit != 0 {
			vals = binary.BigEndian.AppendUint64(vals, max)
		}
	}
	f.AttrVals = append(vals, f.AttrVals[end:]...)
	if len(f.Attrmask) == 0 {
		f.Attrmask = []uint32{0}
	}
	f.Attrmask[0] = have | want
}

// word0End returns where in vals the values of the attributes in mask, a
// first bitmap word, end. It is false when mask has an attribute of unknown
// size or vals are too short to hold what mask says.
func word0End(mask uint32, vals []byte) (int, bool) {
	end := 0
	for n := uint(0); n < 32; n++ {
		if mask&(1<<n) == 0 {
			continue
		}
		size, known := word0Size[n]
		switch {
		case known:
		case n == nfsv4_xdr.FATTR4_SUPPORTED_ATTRS, n == nfsv4_xdr.FATTR4_FILEHANDLE:
			if end+4 > len(vals) {
				return 0, false
			}
			count := int(binary.BigEndian.Uint32(vals[end:]))
			if count < 0 || count > len(vals) {
				return 0, false
			}
			if n == nfsv4_xdr.FATTR4_SUPPORTED_ATTRS {
				size = 4 + 4*count // a count of words
			} else {
				size = 4 + (count+3)/4*4 // a count of bytes, padded
			}
		default:
			return 0, false
		}
		end += size
		if end > len(vals) {
			return 0, false
		}
	}
	return end, true
}
