.PHONY: build tidy test run-local stop k8s-deploy k8s-undeploy clean

build:
	cd collector && go build ./...

tidy:
	cd collector && go mod tidy

test:
	cd collector && go test ./...

run-local:
	docker compose up --build

stop:
	docker compose down

k8s-deploy:
	kubectl apply -f k8s/etcd.yaml
	kubectl apply -f k8s/nats.yaml
	kubectl apply -f k8s/collector.yaml
	kubectl apply -f k8s/analysis.yaml

k8s-undeploy:
	kubectl delete -f k8s/

k8s-build-images:
	docker build -f docker/Dockerfile.collector -t collector:latest ./collector
	docker build -f docker/Dockerfile.analysis  -t analysis:latest  ./analysis
	minikube image load collector:latest
	minikube image load analysis:latest

clean:
	docker compose down -v
	rm -rf collector/go.sum
