package elasticsearch

import (
	"fmt"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
	"strings"
	"time"
)

// Devops produces QuestDB-specific queries for all the devops query types.
type Devops struct {
	*BaseGenerator
	*devops.Core
}

const EsName = "Elasticsearch"

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
	d.generateMaxCpuHistoQuery(qi, fields, "INTERVAL 1 MINUTE", nHosts, duration, humanLabel)
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
	aggs := fieldsAsAggs("avg", metrics)

	q := fmt.Sprintf(`
		SELECT HISTOGRAM(timestamp, INTERVAL 1 HOUR), hostname, %s
		FROM cpu
		WHERE timestamp BETWEEN %d AND %d
		GROUP BY 1, 2
		ORDER BY 1, 2`,
		strings.Join(aggs, ", "),
		window.StartUnixMillis(), window.EndUnixMillis())

	humanLabel := devops.GetDoubleGroupByLabel(EsName, numMetrics)
	fillInQuery(qi, humanLabel, humanDesc(humanLabel, window), q)
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *Devops) LastPointPerHost(qi query.Query) {
	// TODO
}


// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for N random
// hosts
//
// Queries:
// cpu-max-all-1
// cpu-max-all-8
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int, duration time.Duration) {
	humanLabel := devops.GetMaxAllLabel(EsName, nHosts)
	d.generateMaxCpuHistoQuery(qi, devops.GetAllCPUMetrics(), "INTERVAL 1 HOUR", nHosts, duration, humanLabel)
}

// GroupByOrderByLimit populates a query.Query that has a time WHERE clause,
// that groups by a truncated date, orders by that date, and takes a limit:
//
// Queries:
// groupby-orderby-limit
// NOTE: arbitrary `usage_user` field and LIMIT value
func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	window := d.Interval.MustRandWindow(time.Hour)

	q := fmt.Sprintf(`
		SELECT HISTOGRAM(timestamp, INTERVAL 1 MINUTE), MAX(usage_user)
		FROM cpu
		WHERE timestamp < '%s'
		GROUP BY 1
		ORDER BY 1
		LIMIT 5`,
		window.EndString())

	humanLabel := EsName + " max cpu over last 5 min-intervals (random end)"
	fillInQuery(qi, humanLabel, humanDesc(humanLabel, window), q)
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

	q := fmt.Sprintf(`
		SELECT *
		FROM cpu
		WHERE usage_user > 90.0 AND timestamp BETWEEN %d AND %d`,
		window.StartUnixMillis(), window.EndUnixMillis())

	if (nHosts > 0) {
		hosts, err := d.GetRandomHosts(nHosts)
		panicIfErr(err)
		q += fmt.Sprintf(" AND hostname IN ('%s')", strings.Join(hosts, "', '"))
	}

	humanLabel, err := devops.GetHighCPULabel(EsName, nHosts)
	panicIfErr(err)
	fillInQuery(qi, humanLabel, humanDesc(humanLabel, window), q)
}


// TODO: Remove the need for this by continuing to bubble up errors
func panicIfErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func fillInQuery(qi query.Query, humanLabel, HumanDesc, q string) {
	esQuery, ok := qi.(*query.Elasticsearch)
	if !ok {
		panic(fmt.Errorf("can't infer an Elasticsearch type from query %T", qi))
	}
	esQuery.HumanLabel = []byte(humanLabel)
	esQuery.HumanDescription = []byte(HumanDesc)
	esQuery.Query = []byte(q)
}

func (d *Devops) generateMaxCpuHistoQuery(qi query.Query, fields []string, interval string, nHosts int, duration time.Duration,
	humanLabel string) {
	window := d.Interval.MustRandWindow(duration)
	hosts, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)
	aggs := fieldsAsAggs("max", fields)

	q := fmt.Sprintf(`
		SELECT HISTOGRAM(timestamp, %s), %s
		FROM cpu
		WHERE hostname IN ('%s') AND timestamp BETWEEN %d AND %d
		GROUP BY 1
		ORDER BY 1`,
		interval,
		strings.Join(aggs, ", "),
		strings.Join(hosts, "', '"),
		window.StartUnixMillis(), window.EndUnixMillis())

	fillInQuery(qi, humanLabel, humanDesc(humanLabel, window), q)
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

