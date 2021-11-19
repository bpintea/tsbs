package elasticsearch

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	queryUtils "github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
	"time"
)

// BaseGenerator contains settings specific for Elasticsearch.
type BaseGenerator struct {
}

func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewElasticsearch()
}

func (g *BaseGenerator) NewDevops(start, end time.Time, scale int) (queryUtils.QueryGenerator, error) {
	core, err := devops.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	devops := &Devops{
		BaseGenerator: g,
		Core:          core,
	}

	return devops, nil
}

