package main

import (
	"encoding/json"
	"fmt"
	"github.com/blagojts/viper"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
	"strings"
	"time"
)

var (
	runner *query.BenchmarkRunner
	esConfig elasticsearch.Config
)

const FetchSize = 10_000

func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)
	pflag.String("url", "http://localhost:9200", "Elasticsearch URL.")
	pflag.String("user", "", "Authentication user name")
	pflag.String("password", "", "Authentication password")
	pflag.Parse()

	err := utils.SetupConfigFile()
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
	esConfig = elasticsearch.Config{
		Addresses: []string{
			viper.GetString("url"),
		},
		Username: viper.GetString("user"),
		Password: viper.GetString("password"),
	}

	runner = query.NewBenchmarkRunner(config)
}

func main() {
	runner.Run(&query.ElasticsearchPool, func () query.Processor {
		return &processor{}
	})
}

type processor struct {
	es *elasticsearch.Client
}

type SqlQuery struct {
	Query		string	`json:"query"`
	FetchSize	int		`json:"fetch_size"`
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	stat := query.GetStat()
	esQuery := q.(*query.Elasticsearch)

	if runner.Debug > 0 {
		fmt.Printf("Running query: %s\n", esQuery)
	}

	enc, err := json.Marshal(SqlQuery{string(esQuery.Query), FetchSize})
	if err != nil {
		panic(fmt.Errorf("failed to encode query [%s]: %s", esQuery.Query, err))
	}
	start := time.Now()

	res, err := p.es.SQL.Query(strings.NewReader(string(enc)))
	if err != nil {
		panic(fmt.Errorf("failed to run SELECT: %s", err))
	}
	defer res.Body.Close()
	if res.IsError() {
		panic(fmt.Errorf("unsuccesful SQL query: %s", res.Status()))
	}
	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		panic(fmt.Errorf("failed to decode JSON answer: %s", err))
	}
	if _, in := r["cursor"]; in { // TODO: do the tests fetch more than 10K "results"?
		panic(fmt.Errorf("scrolling not currently implemented: %s", err))
	}

	if runner.Debug > 0 {
		fmt.Printf("Received response: %s\n", r)
	}

	// Accounting
	stat.Init(q.HumanLabelName(), float64(time.Since(start).Nanoseconds()) / 1e6)
	return []*query.Stat{stat}, nil
}

func (p *processor) Init(id int) {
	var err error
	if p.es, err = elasticsearch.NewClient(esConfig); err != nil {
		panic(fmt.Errorf("failed to configure Elasticsearch client (%d): %s", id, err))
	}
}