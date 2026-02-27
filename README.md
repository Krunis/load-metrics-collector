# load-metrics-collector
![alt text](static/blockschema.png)

Представляет собой слой приёмки метрик для высоконагруженной телеметрии:
- collector принимает метрики по stream'у и отправляет async producerом в Kafka:
```sh
msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return stream.SendAndClose(&pb.MetricResponse{Acknowledged: true})
			}
		}

		c.metricCh <- msg
//
_case metric = <-c.metricCh:
			valueJSON, _ := json.Marshal(common.MetricForAggr{
				Service: metric.GetService(),
				Metric: metric.GetMetric(),
				Value: metric.GetValue(),
				TimestampUnix: metric.GetTimestamp(),
			})

			c.saramaProducer.SendMsg(
				"raw-metrics",
				[]byte(metric.GetService() + ":" + metric.GetMetric()),
				valueJSON,
			)
```

- worker читает сообщения из Kafka формирует батч и записывает в канал, отдельный цикл читает батчи и изменяет данные этого бакета метрик, считая поля: Count, Sum (для среднего значения) Min, Max. 


docker-compose --profile tools run --rm protobuf_gen
