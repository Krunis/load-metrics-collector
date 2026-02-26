package worker

import (
	"encoding/json"

	"github.com/IBM/sarama"
	"github.com/Krunis/load-metrics-collector/packages/common"
)

type AggrKey struct {
	Service string
	Metric  string
	Bucket  int64
}

type Accumulator struct {
	Count int
	Sum   float32
	Min   float32
	Max   float32
	Avg   float32
	P95   int
}

func (w *Worker) AggregateBatch(batch []*sarama.ConsumerMessage) {
    for _, metric := range batch{
        var m common.MetricForAggr

        json.Unmarshal(metric.Value, &m)

        bucket := m.TimestampUnix / 1000

        key := AggrKey{
            Service: m.Service,
            Metric: m.Metric,
            Bucket: bucket,
        }

        w.AccMap[key].Add(m.Value)
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
}

func (w *Worker) FindP95(batch []*sarama.ConsumerMessage) (int, error) {
	return 0, nil
}
