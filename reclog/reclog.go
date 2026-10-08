// Package reclog is an append-only file of records: what the sidecar has to
// remember from one run to the next, the pins and the handles, is written
// through it.
//
// A record on disk is its length as a uvarint, its bytes, and the CRC-32C
// of the bytes in four bytes, big-endian. A process that dies while it
// writes leaves a last record that is cut short; Open drops it, and
// anything else at the end of the file that is no whole record, and cuts
// the file back to the last record that is.
package reclog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// MaxRecord is the largest record a log takes. A length above it in a file
// is no record of this package's.
const MaxRecord = 1 << 20

// ErrClosed is returned by a log that was closed.
var ErrClosed = errors.New("reclog: log is closed")

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Log is a file of records, open to be appended to. It is safe for
// concurrent use.
type Log struct {
	mu sync.Mutex
	f  *os.File // nil once closed
	w  *bufio.Writer
}

// Open reads every whole record of the file at path, creating the file if
// it is not there, cuts off what follows the last whole record, and returns
// the log ready to be appended to, with the records in the order they were
// written.
func Open(path string) (*Log, [][]byte, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("reclog: %w", err)
	}
	recs, end, err := readAll(f)
	if err == nil {
		// Truncate also when there is nothing to cut: it costs nothing
		// and there is one path through here.
		err = f.Truncate(end)
	}
	if err == nil {
		_, err = f.Seek(end, io.SeekStart)
	}
	if err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("reclog: %s: %w", path, err)
	}
	return &Log{f: f, w: bufio.NewWriterSize(f, 64<<10)}, recs, nil
}

// counter counts the bytes read through it, so that the end of the last
// whole record is known.
type counter struct {
	r *bufio.Reader
	n int64
}

func (c *counter) ReadByte() (byte, error) {
	b, err := c.r.ReadByte()
	if err == nil {
		c.n++
	}
	return b, err
}

func (c *counter) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// readAll returns the whole records of f and the offset at which the last
// of them ends. Only a failure to read is an error: a record that is cut
// short, too long or wrong ends the records.
func readAll(f *os.File) (recs [][]byte, end int64, err error) {
	in := &counter{r: bufio.NewReaderSize(f, 64<<10)}
	var sum [4]byte
	for {
		n, err := binary.ReadUvarint(in)
		if err != nil || n > MaxRecord {
			return recs, end, readFailure(err)
		}
		rec := make([]byte, n)
		if _, err := io.ReadFull(in, rec); err != nil {
			return recs, end, readFailure(err)
		}
		if _, err := io.ReadFull(in, sum[:]); err != nil {
			return recs, end, readFailure(err)
		}
		if binary.BigEndian.Uint32(sum[:]) != crc32.Checksum(rec, castagnoli) {
			return recs, end, nil
		}
		recs = append(recs, rec)
		end = in.n
	}
}

// readFailure tells the end of the file, and a length that is none, from a
// read that failed.
func readFailure(err error) error {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return nil
	}
	var perr *os.PathError
	if errors.As(err, &perr) {
		return err
	}
	// What is left is binary.ReadUvarint's complaint about a length that
	// overflows: bytes that are no record.
	return nil
}

// Append adds a record. It may stay in the log's buffer until Flush, Sync
// or Close.
func (l *Log) Append(rec []byte) error {
	if len(rec) > MaxRecord {
		return fmt.Errorf("reclog: a record of %d bytes, the largest is %d", len(rec), MaxRecord)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return ErrClosed
	}
	var head [binary.MaxVarintLen64]byte
	var sum [4]byte
	binary.BigEndian.PutUint32(sum[:], crc32.Checksum(rec, castagnoli))
	// bufio.Writer keeps its first error and returns it from every write
	// after, the last of these among them.
	l.w.Write(head[:binary.PutUvarint(head[:], uint64(len(rec)))])
	l.w.Write(rec)
	_, err := l.w.Write(sum[:])
	return err
}

// Flush hands what was appended to the file.
func (l *Log) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return ErrClosed
	}
	return l.w.Flush()
}

// Sync hands what was appended to the file and waits for the disk to have
// it.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return ErrClosed
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.f.Sync()
}

// Close hands what was appended to the file and closes it. Closing a log
// twice is no error.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	err := errors.Join(l.w.Flush(), l.f.Close())
	l.f = nil
	return err
}
