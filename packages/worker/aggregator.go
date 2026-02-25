package worker

import "github.com/IBM/sarama"

type AggrKey string

type Accumulator struct {
    Bucket int64
	Count int
	Sum   float64
	Min   float64
	Max   float64
    Avg float64
	P95   int
}

func (a *Accumulator) Add(value float64){
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

func (w *Worker) FindP95(batch []*sarama.ConsumerMessage) (int, error){
	return 0, nil
}