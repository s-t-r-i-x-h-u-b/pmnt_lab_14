.PHONY: build build-rust-cgo build-rust-py tidy test run-local stop k8s-deploy k8s-undeploy k8s-build-images clean

# Build the Rust static library and copy it into the cgo package directory.
build-rust-cgo:
	cd validator && cargo build --release --no-default-features
	cp validator/target/release/libair_quality_validator.a \
	   collector/internal/validator/libair_quality_validator.a
	cp validator/validator.h \
	   collector/internal/validator/validator.h

# Build the PyO3 wheel for local Python development.
build-rust-py:
	cd validator && maturin build --release --features python --out target/wheels

build: build-rust-cgo
	cd collector && CGO_ENABLED=1 go build -tags rust_validator ./...

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

k8s-build-images: build-rust-cgo
	docker build -f docker/Dockerfile.collector -t collector:latest .
	docker build -f docker/Dockerfile.analysis  -t analysis:latest  .
	minikube image load collector:latest
	minikube image load analysis:latest

clean:
	docker compose down -v
	rm -rf collector/go.sum
	rm -f collector/internal/validator/libair_quality_validator.a
	rm -f collector/internal/validator/validator.h
