.PHONY: proxy pub-metrics

proxy:
	go build -o build/liteserver cmd/main.go

pub-metrics:
	go build -o build/pub-metrics cmd/pub-metric/main.go
