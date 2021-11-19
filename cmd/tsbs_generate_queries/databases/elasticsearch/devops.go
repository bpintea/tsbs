package elasticsearch

import (
	"fmt"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
	"strings"
	"text/template"
	"time"
)

// Devops produces QuestDB-specific queries for all the devops query types.
type Devops struct {
	*BaseGenerator
	*devops.Core
}

const (
	EsName = "Elasticsearch"
	TargetIndex = "cpu"
)

var (
	maxCpuHistoTmpl *template.Template
	groupByTimeAndPrimaryTagTmpl *template.Template
	highCPUForHostsNoHostFilterTmpl *template.Template
	highCPUForHostsWithHostFilterTmpl *template.Template
)

func init() {
	t := template.New("maxCpuHistoTemplate")
	t = t.Funcs(template.FuncMap{"ToQuotedList": ToQuotedList})
	t = t.Funcs(template.FuncMap{"FieldsAsAggs": FieldsAsAggs})
	var err error
	if maxCpuHistoTmpl, err = t.Parse(maxCpuHistoTemplate); err != nil {
		panic(fmt.Errorf("failed parsing maxCpuHistoTemplate template: %s", err))
	}

	t = template.New("groupByTimeAndPrimaryTagTemplate")
	t = t.Funcs(template.FuncMap{"FieldsAsAggs": FieldsAsAggs})
	if groupByTimeAndPrimaryTagTmpl, err = t.Parse(groupByTimeAndPrimaryTagTemplate); err != nil {
		panic(fmt.Errorf("failed parsing groupByTimeAndPrimaryTagTemplate template: %s", err))
	}

	t = template.New("highCPUForHostsNoHostFilterTemplate")
	tmpl := fmt.Sprintf(highCPUForHostsTemplateFormat, "")
	if highCPUForHostsNoHostFilterTmpl, err = t.Parse(tmpl); err != nil {
		panic(fmt.Errorf("failed parsing highCPUForHostsNoHostFilterTmpl template: %s", err))
	}

	t = template.New("highCPUForHostsWithHostFilterTemplate")
	t = t.Funcs(template.FuncMap{"ToQuotedList": ToQuotedList})
	tmpl = fmt.Sprintf(highCPUForHostsTemplateFormat, highCPUForHostsFilterTemplate)
	if highCPUForHostsWithHostFilterTmpl, err = t.Parse(tmpl); err != nil {
		panic(fmt.Errorf("failed parsing highCPUForHostsWithHostFilterTmpl template: %s", err))
	}
}

type IntervalType int
const (
	IntervalMinute IntervalType = iota
	IntervalHour
)

// GroupByTime selects the MAX for metrics under 'cpu', per minute for N random
// hosts
//
// Resultsets:
// single-groupby-1-1-12
// single-groupby-1-1-1
// single-groupby-1-8-1
// single-groupby-5-1-12
// single-groupby-5-1-1
// single-groupby-5-8-1
func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, duration time.Duration) {
	fields, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	humanLabel := fmt.Sprintf(EsName+ " %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, duration)
	d.generateMaxCpuHistoQuery(qi, fields, IntervalMinute, nHosts, duration, humanLabel)
}

// GroupByTimeAndPrimaryTag selects the AVG of metrics in the group `cpu` per device
// per hour for a day
//
// Queries:
// double-groupby-1
// double-groupby-5
// double-groupby-all
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	window := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)

	// generate SQL
	aggs := fieldsAsAggs("avg", metrics)
	sql := fmt.Sprintf(`
		SELECT HISTOGRAM(timestamp, INTERVAL 1 HOUR), hostname, %s
		FROM %s
		WHERE timestamp BETWEEN %d AND %d
		GROUP BY 1, 2
		ORDER BY 1, 2`,
		strings.Join(aggs, ", "),
		TargetIndex,
		window.StartUnixMillis(), window.EndUnixMillis())

	// generate DSL
	var builder strings.Builder
	err = groupByTimeAndPrimaryTagTmpl.Execute(&builder, GroupByTimeAndPrimaryTagParams{
		window.StartUnixMillis(),
		window.EndUnixMillis(),
		metrics})
	if err != nil {
		panic(fmt.Errorf("failed to execute groupByTimeAndPrimaryTagTmpl template: %s", err))
	}
	dsl := builder.String()

	humanLabel := devops.GetDoubleGroupByLabel(EsName, numMetrics)
	fillInQuery(qi, humanLabel, humanDesc(humanLabel, window), sql, dsl)
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *Devops) LastPointPerHost(qi query.Query) {
	sql := "" // ES/SQL can't yet express this query
	dsl := lastPointPerHostQuery
	humanLabel := EsName + " last row per host"
	humanDesc := humanLabel
	fillInQuery(qi, humanLabel, humanDesc, sql, dsl)
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for N random
// hosts
//
// Queries:
// cpu-max-all-1
// cpu-max-all-8
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int, duration time.Duration) {
	fields := devops.GetAllCPUMetrics()
	humanLabel := devops.GetMaxAllLabel(EsName, nHosts)
	d.generateMaxCpuHistoQuery(qi, fields, IntervalHour, nHosts, duration, humanLabel)
}

// GroupByOrderByLimit populates a query.Query that has a time WHERE clause,
// that groups by a truncated date, orders by that date, and takes a limit:
//
// Queries:
// groupby-orderby-limit
// NOTE: arbitrary `usage_user` field and LIMIT value
func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	window := d.Interval.MustRandWindow(time.Hour)

	sql := fmt.Sprintf(`
		SELECT HISTOGRAM(timestamp, INTERVAL 1 MINUTE), MAX(usage_user)
		FROM %s
		WHERE timestamp < '%s'
		GROUP BY 1
		ORDER BY 1
		LIMIT 5`,
		TargetIndex,
		window.EndString())

	dsl := fmt.Sprintf(groupByOrderByLimitFormat, window.EndString())

	humanLabel := EsName + " max cpu over last 5 min-intervals (random end)"
	fillInQuery(qi, humanLabel, humanDesc(humanLabel, window), sql, dsl)
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has
// high usage between a time period for a number of hosts (if 0, it will
// search all hosts)
//
// Queries:
// high-cpu-1
// high-cpu-all
// NOTE: arbitrary `usage_user` field
func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	window := d.Interval.MustRandWindow(devops.HighCPUDuration)

	var err error
	hosts := []string{}
	if nHosts > 0 {
		hosts, err = d.GetRandomHosts(nHosts)
		panicIfErr(err)
	}

	// generate SQL
	sql := fmt.Sprintf(`
		SELECT *
		FROM %s
		WHERE usage_user > 90.0 AND timestamp BETWEEN %d AND %d`,
		TargetIndex,
		window.StartUnixMillis(), window.EndUnixMillis())

	if (len(hosts) > 0) {
		sql += fmt.Sprintf(" AND hostname IN ('%s')", strings.Join(hosts, "', '"))
	}

	// generate DSL
	var tmpl *template.Template
	if len(hosts) > 0 {
		tmpl = highCPUForHostsWithHostFilterTmpl
	} else {
		tmpl = highCPUForHostsNoHostFilterTmpl
	}
	var builder strings.Builder
	err = tmpl.Execute(&builder, HighCPUForHostsParams{
		window.StartUnixMillis(),
		window.EndUnixMillis(),
		hosts,
	})
	if err != nil {
		panic(fmt.Errorf("failed to execute highCPUForHostsTmpl template: %s", err))
	}
	dsl := builder.String()

	humanLabel, err := devops.GetHighCPULabel(EsName, nHosts)
	panicIfErr(err)
	fillInQuery(qi, humanLabel, humanDesc(humanLabel, window), sql, dsl)
}


// TODO: Remove the need for this by continuing to bubble up errors
func panicIfErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func fillInQuery(qi query.Query, humanLabel, HumanDesc, sql, dsl string) {
	esQuery, ok := qi.(*query.Elasticsearch)
	if !ok {
		panic(fmt.Errorf("can't infer an Elasticsearch type from query %T", qi))
	}
	esQuery.HumanLabel = []byte(humanLabel)
	esQuery.HumanDescription = []byte(HumanDesc)
	esQuery.SqlQuery = []byte(sql)
	esQuery.DslQuery = []byte(dsl)
	esQuery.Index = []byte(TargetIndex)
}

func (d *Devops) generateMaxCpuHistoQuery(qi query.Query, fields []string, intervalType IntervalType, nHosts int, duration time.Duration,
	humanLabel string) {
	window := d.Interval.MustRandWindow(duration)
	hosts, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)

	var intervalString string
	var intervalMillis int
	if intervalType == IntervalMinute {
		intervalString = "INTERVAL 1 MINUTE"
		intervalMillis = 60 * 1e3
	} else {
		intervalString = "INTERVAL 1 HOUR"
		intervalMillis = 3600 * 1e3
	}

	// generate SQL
	aggs := fieldsAsAggs("max", fields)
	sql := fmt.Sprintf(`
		SELECT HISTOGRAM(timestamp, %s), %s
		FROM %s
		WHERE hostname IN ('%s') AND timestamp BETWEEN %d AND %d
		GROUP BY 1
		ORDER BY 1`,
		intervalString,
		strings.Join(aggs, ", "),
		TargetIndex,
		strings.Join(hosts, "', '"),
		window.StartUnixMillis(), window.EndUnixMillis())

	// generate DSL
	var builder strings.Builder
	err = maxCpuHistoTmpl.Execute(&builder, MaxCpuHistoParams{
		hosts,
		window.StartUnixMillis(),
		window.EndUnixMillis(),
		intervalMillis,
		fields})
	if err != nil {
		panic(fmt.Errorf("failed to execute maxCpuHistoTmpl template: %s", err))
	}
	dsl := builder.String()

	fillInQuery(qi, humanLabel, humanDesc(humanLabel, window), sql, dsl)
}

func humanDesc(humanLabel string, window *utils.TimeInterval) string {
	return fmt.Sprintf("%s: %s to %s", humanLabel, window.StartString(), window.EndString())
}

// fieldsAsAggs builds specified aggregate function clauses for
// a set of column idents.
//
// For instance:
//      max(cpu_time) AS max_cpu_time
func fieldsAsAggs(aggFunc string, idents []string) []string {
	selectAggClauses := make([]string, len(idents))
	for i, ident := range idents {
		selectAggClauses[i] =
			fmt.Sprintf("%[1]s(%[2]s) AS %[1]s_%[2]s", aggFunc, ident)
	}
	return selectAggClauses
}

func FieldsAsAggs(agg string, fields []string) string {
	out := ""
	for _, field := range fields {
		out += fmt.Sprintf(`"%[1]s_agg":{"%[2]s":{"field":"%[1]s"}},`, field, agg)
	}
	return out[:len(out) - 1]
}

func ToQuotedList(elems []string) string {
	out := ""
	for _, elem := range elems {
		out += "\"" + elem + "\","
	}
	return out[:len(out) - 1]
}

type MaxCpuHistoParams struct {
	Hostnames []string
	From int64
	To int64
	HistoMillis int
	Fields []string
}

const maxCpuHistoTemplate = `
{
    "size": 0,
    "query": {
        "bool": {
            "must": [
                {
                    "terms": {
                        "hostname.keyword": [{{ToQuotedList .Hostnames}}],
                        "boost": 1.0
                    }
                },
                {
                    "range": {
                        "timestamp": {
                            "from": {{.From}},
                            "to": {{.To}},
                            "include_lower": true,
                            "include_upper": true,
                            "time_zone": "Z",
                            "boost": 1.0
                        }
                    }
                }
            ],
            "boost": 1.0
        }
    },
    "_source": false,
    "aggregations": {
        "groupby": {
            "composite": {
                "size": 1000,
                "sources": [
                    {
                        "date_histogram_agg": {
                            "date_histogram": {
                                "field": "timestamp",
                                "missing_bucket": true,
                                "order": "asc",
                                "fixed_interval": "{{.HistoMillis}}ms",
                                "time_zone": "Z"
                            }
                        }
                    }
                ]
            },
            "aggregations": { {{FieldsAsAggs "max" .Fields}} }
        }
    }
}`

type GroupByTimeAndPrimaryTagParams struct {
	From int64
	To int64
	Fields []string
}

const groupByTimeAndPrimaryTagTemplate = `
{
    "size": 0,
    "query": {
        "range": {
            "timestamp": {
                "from": {{.From}},
                "to": {{.To}},
                "include_lower": true,
                "include_upper": true,
                "time_zone": "Z",
                "boost": 1.0
            }
        }
    },
    "_source": false,
    "aggregations": {
        "groupby": {
            "composite": {
                "size": 1000,
                "sources": [
                    {
                        "date_histogram_agg": {
                            "date_histogram": {
                                "field": "timestamp",
                                "missing_bucket": true,
                                "order": "asc",
                                "fixed_interval": "3600000ms",
                                "time_zone": "Z"
                            }
                        }
                    },
                    {
                        "hostname_agg": {
                            "terms": {
                                "field": "hostname.keyword",
                                "missing_bucket": true,
                                "order": "asc"
                            }
                        }
                    }
                ]
            },
            "aggregations": { {{FieldsAsAggs "avg" .Fields}} }
        }
    }
}`

const groupByOrderByLimitFormat = `
{
    "size": 0,
    "query": {
        "range": {
            "timestamp": {
                "from": null,
                "to": "%s",
                "include_lower": false,
                "include_upper": false,
                "time_zone": "Z",
                "boost": 1.0
            }
        }
    },
    "_source": false,
    "aggregations": {
        "groupby": {
            "composite": {
                "size": 5,
                "sources": [
                    {
                        "date_histogram_agg": {
                            "date_histogram": {
                                "field": "timestamp",
                                "missing_bucket": true,
                                "order": "asc",
                                "fixed_interval": "60000ms",
                                "time_zone": "Z"
                            }
                        }
                    }
                ]
            },
            "aggregations": {
                "usage_user_agg": {
                    "max": {
                        "field": "usage_user"
                    }
                }
            }
        }
    }
}`

type HighCPUForHostsParams struct {
	From int64
	To int64
	Hostnames []string
}

const highCPUForHostsTemplateFormat = `
{
    "size": 1000,
    "query": {
        "bool": {
            "must": [
                {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "usage_user": {
                                        "from": 90.0,
                                        "to": null,
                                        "include_lower": false,
                                        "include_upper": false,
                                        "boost": 1.0
                                    }
                                }
                            },
                            {
                                "range": {
                                    "timestamp": {
                                        "from": {{.From}},
                                        "to": {{.To}},
                                        "include_lower": true,
                                        "include_upper": true,
                                        "time_zone": "Z",
                                        "boost": 1.0
                                    }
                                }
                            }
                        ],
                        "boost": 1.0
                    }
                }%s
            ],
            "boost": 1.0
        }
    },
    "_source": false,
    "fields": [
        {"field": "arch"},
        {"field": "datacenter"},
        {"field": "hostname"},
        {"field": "os"},
        {"field": "rack"},
        {"field": "region"},
        {"field": "service"},
        {"field": "service_environment"},
        {"field": "service_version"},
        {"field": "team"},
        {"field": "timestamp"},
        {"field": "usage_guest"},
        {"field": "usage_guest_nice"},
        {"field": "usage_idle"},
        {"field": "usage_iowait"},
        {"field": "usage_irq"},
        {"field": "usage_nice"},
        {"field": "usage_softirq"},
        {"field": "usage_steal"},
        {"field": "usage_system"},
        {"field": "usage_user"}
    ],
    "sort": [
        {
            "_doc": {
                "order": "asc"
            }
        }
    ]
}`

const highCPUForHostsFilterTemplate = `,
                {
                    "terms": {
                        "hostname.keyword": [{{ToQuotedList .Hostnames}}],
                        "boost": 1.0
                    }
                }
`

const lastPointPerHostQuery = `
{
    "size": 0,
    "aggs": {
        "hostname_agg": {
            "terms": {
                "field": "hostname.keyword"
            },
            "aggs": {
                "latest_timestamp": {
                    "top_hits": {
                        "sort": [
                            {
                                "timestamp": {
                                    "order": "desc"
                                }
                            }
                        ],
                        "size": 1,
                        "_source": false,
                        "fields": [
                            {"field": "arch"},
                            {"field": "datacenter"},
                            {"field": "hostname"},
                            {"field": "os"},
                            {"field": "rack"},
                            {"field": "region"},
                            {"field": "service"},
                            {"field": "service_environment"},
                            {"field": "service_version"},
                            {"field": "team"},
                            {"field": "timestamp"},
                            {"field": "usage_guest"},
                            {"field": "usage_guest_nice"},
                            {"field": "usage_idle"},
                            {"field": "usage_iowait"},
                            {"field": "usage_irq"},
                            {"field": "usage_nice"},
                            {"field": "usage_softirq"},
                            {"field": "usage_steal"},
                            {"field": "usage_system"},
                            {"field": "usage_user"}
                        ]
                    }
                }
            }
        }
    }
}`