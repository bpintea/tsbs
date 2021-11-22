// tsbs_load_elasticsearch loads Elasticsearch with data from stdin or file.

package main

import (
	"fmt"
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets/elasticsearch"
	"log"
	"strings"
)

// Program option vars:
var (
	esURLs []string
)

func initProgramOptions() (*elasticsearch.SpecificConfig, load.BenchmarkRunner, *load.BenchmarkRunnerConfig) {
	target := elasticsearch.NewTarget()

	loaderConf := load.BenchmarkRunnerConfig{}
	loaderConf.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("", pflag.CommandLine)
	pflag.Parse()

	if err := utils.SetupConfigFile(); err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	if err := viper.Unmarshal(&loaderConf); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	urls := viper.GetString("urls")
	if len(urls) == 0 {
		log.Fatalf("missing `urls` flag")
	}
	esURLs := strings.Split(urls, ",")

	loader := load.GetBenchmarkRunner(loaderConf)
	return &elasticsearch.SpecificConfig{ServerURLs: esURLs}, loader, &loaderConf
}

func main() {
	esSpecificConf, loader, loaderConf := initProgramOptions()

	benchmark, err := elasticsearch.NewBenchmark(esSpecificConf, &source.DataSourceConfig{
		Type: source.FileDataSourceType,
		File: &source.FileDataSourceConfig{Location: loaderConf.FileName},
	})
	if err != nil {
		panic(err)
	}
	loader.RunBenchmark(benchmark)
}
