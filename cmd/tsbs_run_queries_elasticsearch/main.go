package main

import (
	"encoding/json"
	"fmt"
	"github.com/blagojts/viper"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
	"io"
	"strings"
	"time"
)

var (
	runner *query.BenchmarkRunner
	esConfig  elasticsearch.Config
	queryLang string
)

const FetchSize = 10_000

func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)
	pflag.String("url", "http://localhost:9200", "Elasticsearch URL.")
	pflag.String("user", "", "Authentication user name")
	pflag.String("password", "", "Authentication password")
	pflag.String("query-lang", "dsl", "Query language: sql or dsl")
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
	queryLang = viper.GetString("query-lang")

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

	if queryLang == "sql" && len(esQuery.SqlQuery) <= 0 || queryLang == "dsl" && len(esQuery.DslQuery) <= 0 {
		panic(fmt.Errorf("language \"%s\" not supported with chosen query type: \"%s\"", queryLang, esQuery.HumanLabel))
	}

	var start time.Time
	if queryLang == "sql" {
		start = p.ProcessSql(esQuery)
	} else {
		start = p.ProcessDsl(esQuery)
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

func (p *processor) ProcessSql(esQuery *query.Elasticsearch) time.Time {
	enc, err := json.Marshal(SqlQuery{string(esQuery.SqlQuery), FetchSize})
	if err != nil {
		panic(fmt.Errorf("failed to encode query [%s]: %s", esQuery.SqlQuery, err))
	}
	start := time.Now()

	var res *esapi.Response
	if runner.Debug > 0 {
		// TODO: is there a way to set request params when just using es.SQL.Query()?
		sqlRequest := esapi.SQLQueryRequest{
			Body: strings.NewReader(string(enc)),
			Pretty: true,
			ErrorTrace: true,
		}
		res, err = sqlRequest.Do(nil, p.es.Transport)
	} else {
		res, err = p.es.SQL.Query(strings.NewReader(string(enc)))
	}
	if err != nil {
		panic(fmt.Errorf("failed to run SQL query: %s", err))
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Printf("Failed to close response body for query: %s", esQuery.SqlQuery)
		}
	}(res.Body)


	jsonRes := decodeAnswer(res)
	if _, in := jsonRes["cursor"]; in { // TODO: do the tests fetch more than 10K "results"?
		// panic(fmt.Errorf("scrolling not currently implemented"))
		// TODO: oftentimes the following the scroll yields an empty rowset, which however taxes the timing
		fmt.Printf("WARNING: scrolling not currently implemented!\n")
	}

	if runner.Debug > 0 {
		fmt.Printf("Received response: %s\n", jsonRes)
	}

	return start
}

func (p *processor) ProcessDsl(esQuery *query.Elasticsearch) time.Time {
	start := time.Now()
	res, err := p.es.Search(
		p.es.Search.WithIndex(string(esQuery.Index)),
		p.es.Search.WithBody(strings.NewReader(string(esQuery.DslQuery))))
	if err != nil {
		panic(fmt.Errorf("failed to run DSL query: %s", err))
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(res.Body)

	jsonRes := decodeAnswer(res)

	if runner.Debug > 0 {
		fmt.Printf("Received response: %s\n", jsonRes)
	}
	return start
}

func decodeAnswer(res *esapi.Response) map[string]interface{} {
	if res.IsError() {
		if runner.Debug > 0 {
			fmt.Printf("failed query answer: %s", res)
		}
		panic(fmt.Errorf("unsuccesful query: %s", res.Status()))
	}
	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		panic(fmt.Errorf("failed to decode JSON answer: %s", err))
	}

	return r
}