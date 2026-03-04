package worker

import (
	"encoding/json"
	"log"
	"math"
	"sort"

	"github.com/IBM/sarama"
	"github.com/Krunis/load-metrics-collector/packages/common"
)

type AggregatedMetric struct {
	Service string  `json:"service"`
	Metric  string  `json:"metric"`
	Bucket  int64   `json:"bucket"` // unix seconds
	Count   int     `json:"count"`
	Min     float32 `json:"min"`
	Max     float32 `json:"max"`
	Avg     float32 `json:"avg"`
	P95     float32 `json:"p95"`
}

type AggrKey struct {
	Service string
	Metric  string
	Bucket  int64 // seconds
}

type Accumulator struct {
	Count int
	Sum   float32
	Min   float32
	Max   float32
	Avg   float32
	P95   float32
	Values []float32
}

func (w *Worker) AggregateBatch(batch []*sarama.ConsumerMessage) {
	for _, metric := range batch {
		var m common.MetricForAggr

		json.Unmarshal(metric.Value, &m)

		key := AggrKey{
			Service: m.Service,
			Metric:  m.Metric,
			Bucket:  m.TimestampUnix / 1000,
		}

		w.AccMapMutex.Lock()

		if _, exists := w.AccMap[key]; !exists{
			w.AccMap[key] = &Accumulator{}
		}

		w.AccMap[key].Add(m.Value)

		log.Printf("Aggregated: %v", key)

		w.AccMapMutex.Unlock()
	}
}

func (a *Accumulator) Add(value float32) {
	if a.Count == 0 {
		a.Min = value
		a.Max = value
	} else {
		if value < a.Min {
			a.Min = value
		}
		if value > a.Max {
			a.Max = value
		}
	}

	a.Sum += value
	a.Count++
	a.Values = append(a.Values, value)
}

func (a *Accumulator) findP95() {
	vals := a.Values

	sort.Slice(vals, func(i, j int) bool {
		return vals[i] < vals[j]
	})
	idx := int(math.Ceil(0.95 * float64(len(vals)))) - 1

	a.P95 = vals[idx]
}