package elasticsearch

import (
	"context"
	"crypto/tls"
	"fmt"
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
	url    string
	esURLs []string
}

func (p *processor) Init(workerNum int, doLoad, hashWorkers bool) {
	p.url = p.esURLs[workerNum%len(p.esURLs)]
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batch := b.(*batch)
	if !doLoad {
		return batch.metrics, batch.rows
	}
	mc, rc := p.do(batch)
	return mc, rc
}

func (p *processor) do(b *batch) (uint64, uint64) {
	cfg := elasticsearch.Config{
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

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}

	// Create the indexer
	//
	indexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:     es,     // The Elasticsearch client
		Index:      "test", // The default index name
		NumWorkers: 4,      // The number of worker goroutines (default: number of CPUs)
		FlushBytes: 5e+6,   // The flush threshold in bytes (default: 5M)
	})
	if err != nil {
		log.Fatalf("Error creating the indexer: %s", err)
	}

	for {
		//r := bytes.NewReader(b.buf.Bytes())

		// Add an item to the indexer
		//
		err = indexer.Add(
			context.Background(),
			esutil.BulkIndexerItem{
				// Action field configures the operation to perform (index, create, delete, update)
				Action: "index",

				// DocumentID is the optional document ID
				//DocumentID: "1",

				// Body is an `io.Reader` with the payload
				Body: strings.NewReader(`{"title":"Test"}`),

				// OnSuccess is the optional callback for each successful operation
				OnSuccess: func(
					ctx context.Context,
					item esutil.BulkIndexerItem,
					res esutil.BulkIndexerResponseItem,
				) {
					fmt.Printf("[%d] %s test/%s", res.Status, res.Result, item.DocumentID)
				},

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

		////////////////

		//req, err := http.NewRequest("POST", p.url, r)
		//if err != nil {
		//	log.Fatalf("error while creating new request: %s", err)
		//}
		//resp, err := http.DefaultClient.Do(req)
		//if err != nil {
		//	log.Fatalf("error while executing request: %s", err)
		//}
		//resp.Body.Close()
		//if resp.StatusCode == http.StatusNoContent {
		//	b.buf.Reset()
		//	return b.metrics, b.rows
		//}

		//
		//log.Printf("server returned HTTP status %d. Retrying", resp.StatusCode)
		//time.Sleep(time.Millisecond * 10)

		if err != nil {
			log.Fatalf("Unexpected error: %s", err)
		}
	}

	// Close the indexer channel and flush remaining items
	//
	if err := indexer.Close(context.Background()); err != nil {
		log.Fatalf("Unexpected error: %s", err)
	}

	// Report the indexer statistics
	//
	stats := indexer.Stats()
	if stats.NumFailed > 0 {
		log.Fatalf("Indexed [%d] documents with [%d] errors", stats.NumFlushed, stats.NumFailed)
	} else {
		log.Printf("Successfully indexed [%d] documents", stats.NumFlushed)
	}

	return b.metrics, b.rows
}
