package elasticsearch

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
)

func NewTarget() targets.ImplementedTarget {
	return &esTarget{}
}

type esTarget struct {
}

func (es esTarget) Benchmark(_ string, dataSourceConfig *source.DataSourceConfig, v *viper.Viper) (targets.Benchmark, error) {
	esSpecificConfig, err := parseSpecificConfig(v)
	if err != nil {
		return nil, err
	}

	return NewBenchmark(esSpecificConfig, dataSourceConfig)
}

func (es esTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (es esTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(
		flagPrefix+"urls",
		"http://localhost:9200/",
		"Comma-separated list of Elasticsearch ingestion URLs",
	)
}

func (es esTarget) TargetName() string {
	return constants.FormatElasticsearch
}
