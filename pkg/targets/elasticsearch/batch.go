package elasticsearch

import (
	"bytes"
	"github.com/timescale/tsbs/pkg/data"
)

var (
	newLine = []byte("\n")
)

type batch struct {
	data    []string
	buf     *bytes.Buffer
	rows    uint64
	metrics uint64
}

func (b *batch) Len() uint {
	return uint(b.rows)
}

func (b *batch) Append(item data.LoadedPoint) {
	that := item.Data.([]byte)
	b.rows++
	//b.buf.Write(that)
	//b.buf.Write(newLine)

	b.data = append(b.data, string(that))
}
