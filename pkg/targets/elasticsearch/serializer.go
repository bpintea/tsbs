package elasticsearch

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"io"
)

// Serializer writes a Point in JSON
type Serializer struct{}

// Serialize writes Point data to the given writer, conforming to the
// to JSON format, supported by Elasticsearch wire protocol
//
// This function writes output that looks like:
// {"name" : "<measurement>", "@timestamp": "<timestamp>", "<tag key>": "<tag value>" "<field name>":<field value>}\n
//
// For example:
// {"name": "foo", "@timestamp": 1451606430000, "tag0": "bar", "baz":-1.0}\n
func (s *Serializer) Serialize(p *data.Point, w io.Writer) (err error) {
	buf := make([]byte, 0, 2048)
	// Add measurement name with in the field "name"
	buf = append(buf, "{\"name\": \""...)
	buf = append(buf, p.MeasurementName()...)
	buf = append(buf, '"')

	// Add the @timestamp field
	buf = append(buf, ", \"@timestamp\": "...)
	buf = serialize.FastFormatAppend(p.Timestamp().UTC().UnixMilli(), buf)

	fakeTags := make([]int, 0)
	tagKeys := p.TagKeys()
	tagValues := p.TagValues()
	for i := 0; i < len(tagKeys); i++ {
		if tagValues[i] == nil {
			continue
		}
		switch v := tagValues[i].(type) {
		case string:
			buf = append(buf, ',', ' ', '"')
			buf = append(buf, tagKeys[i]...)
			buf = append(buf, '"', ':', ' ', '"')
			buf = append(buf, []byte(v)...)
			buf = append(buf, '"')
		default:
			fakeTags = append(fakeTags, i)
		}
	}
	fieldKeys := p.FieldKeys()
	if len(fakeTags) > 0 || len(fieldKeys) > 0 {
		buf = append(buf, ' ')
	}

	for i := 0; i < len(fakeTags); i++ {
		tagIndex := fakeTags[i]

		buf = appendField(buf, tagKeys[tagIndex], tagValues[tagIndex])
	}

	fieldValues := p.FieldValues()
	for i := 0; i < len(fieldKeys); i++ {
		value := fieldValues[i]
		if value == nil {
			continue
		}

		buf = appendField(buf, fieldKeys[i], value)
	}

	buf = append(buf, '}', '\n')
	_, err = w.Write(buf)

	return err
}

func appendField(buf, key []byte, v interface{}) []byte {
	buf = append(buf, ',', ' ', '"')
	buf = append(buf, key...)
	buf = append(buf, "\": "...)

	// If type is string, we should enclose the value in "
	switch v.(type) {
	case string:
		buf = append(buf, '"')
	}
	buf = serialize.FastFormatAppend(v, buf)
	switch v.(type) {
	case string:
		buf = append(buf, '"')
	}

	return buf
}
