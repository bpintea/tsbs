package elasticsearch

import (
	"context"
	"crypto/tls"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/timescale/tsbs/pkg/targets"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

type processor struct {
	esURLs   []string
	esCfg    elasticsearch.Config
	esClient *elasticsearch.Client
}

func (p *processor) Init(workerNum int, doLoad, hashWorkers bool) {
	p.esCfg = elasticsearch.Config{
		Addresses: p.esURLs,
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: time.Second,
			DialContext:           (&net.Dialer{Timeout: time.Second}).DialContext,
			TLSClientConfig: &tls.Config{
				MaxVersion:         tls.VersionTLS11,
				InsecureSkipVerify: true,
			},
		},
	}

	var err error
	p.esClient, err = elasticsearch.NewClient(p.esCfg)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batch := b.(*batch)

	if doLoad == false {
		return batch.metrics, batch.rows
	}

	// Create the indexer
	indexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:     p.esClient, // The Elasticsearch client
		Index:      "test",     // The default index name
		NumWorkers: 4,          // The number of worker goroutines (default: number of CPUs)
		FlushBytes: 5e+6,       // The flush threshold in bytes (default: 5M)
	})
	if err != nil {
		log.Fatalf("Error creating the indexer: %s", err)
	}

	for i := 0; i < len(batch.data); i++ {
		buf := batch.data[i]

		// Add an item to the indexer
		err = indexer.Add(
			context.Background(),
			esutil.BulkIndexerItem{
				Action: "index",

				// Body is an `io.Reader` with the payload
				Body: strings.NewReader(buf),

				// OnFailure is the optional callback for each failed operation
				OnFailure: func(
					ctx context.Context,
					item esutil.BulkIndexerItem,
					res esutil.BulkIndexerResponseItem, err error,
				) {
					if err != nil {
						log.Printf("ERROR: %s", err)
					} else {
						log.Printf("ERROR: %s: %s", res.Error.Type, res.Error.Reason)
					}
				},
			},
		)

		if err != nil {
			log.Fatalf("Unexpected error: %s", err)
		}
	}

	// Close the indexer channel and flush remaining items
	if err := indexer.Close(context.Background()); err != nil {
		log.Fatalf("Unexpected error: %s", err)
	}

	// Report the indexer statistics
	stats := indexer.Stats()
	if stats.NumFailed > 0 {
		log.Fatalf("Indexed [%d] documents with [%d] errors", stats.NumFlushed, stats.NumFailed)
	} else {
		log.Printf("Successfully indexed [%d] documents", stats.NumFlushed)
	}

	return 0, batch.rows
}
