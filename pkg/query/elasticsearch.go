package query

import (
	"fmt"
	"sync"
)

// Elasticsearch encodes an ES REST request.
// This will serialized and piped to tsbs_run_queries_elasticsearch.
type Elasticsearch struct {
	HumanLabel       []byte
	HumanDescription []byte
	SqlQuery         []byte
	DslQuery         []byte
	Index            []byte
	id               uint64
}


// ElasticsearchPool is a sync.Pool of ElasticsearchPool Query types
var ElasticsearchPool = sync.Pool{
	New: func() interface{} {
		return &Elasticsearch{
			HumanLabel:       []byte{},
			HumanDescription: []byte{},
		}
	},
}

// Release resets and returns this Query to its pool
func (es *Elasticsearch) Release() {
	es.HumanLabel = es.HumanLabel[:0]
	es.HumanDescription = es.HumanDescription[:0]
	es.SqlQuery = es.SqlQuery[:0]
	es.DslQuery = es.DslQuery[:0]
	es.id = 0

	ElasticsearchPool.Put(es)
}

// HumanLabelName returns the human readable name of this Query
func (es *Elasticsearch) HumanLabelName() []byte {
	return es.HumanLabel
}

// HumanDescriptionName returns the human readable description of this Query
func (es *Elasticsearch) HumanDescriptionName() []byte {
	return es.HumanDescription
}

// GetID returns the ID of this Query
func (es *Elasticsearch) GetID() uint64 {
	return es.id
}

// SetID sets the ID for this Query
func (es *Elasticsearch) SetID(n uint64) {
	es.id = n
}

// String produces a debug-ready description of a Query.
func (es *Elasticsearch) String() string {
	return fmt.Sprintf("HumanLabel: \"%s\", HumanDescription: \"%s\", SqlQuery: \"%s\", DslQuery: \"%s\", Index: \"%s\"",
		es.HumanLabel, es.HumanDescription, es.SqlQuery, es.DslQuery, es.Index)
}

func NewElasticsearch() *Elasticsearch {
	return ElasticsearchPool.Get().(*Elasticsearch);
}