.PHONY: build build-rust-cgo build-rust-py tidy test run-local stop \
        k8s-deploy k8s-undeploy k8s-build-images \
        k8s-hpa-setup k8s-keda-setup k8s-hpa-status k8s-load-test \
        bench bench-python bench-compare bench-report \
        clean

# ── Rust library ──────────────────────────────────────────────────────────────

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

# ── Go collector ──────────────────────────────────────────────────────────────

build: build-rust-cgo
	cd collector && CGO_ENABLED=1 go build -tags rust_validator ./...

tidy:
	cd collector && go mod tidy

test:
	cd collector && go test ./...

# ── Docker Compose (local) ────────────────────────────────────────────────────

run-local:
	docker compose up --build

stop:
	docker compose down

# ── Kubernetes ────────────────────────────────────────────────────────────────

k8s-build-images: build-rust-cgo
	docker build -f docker/Dockerfile.collector -t collector:latest .
	docker build -f docker/Dockerfile.analysis  -t analysis:latest  .
	minikube image load collector:latest
	minikube image load analysis:latest

k8s-deploy:
	kubectl apply -f k8s/etcd.yaml
	kubectl apply -f k8s/nats.yaml
	kubectl apply -f k8s/collector.yaml
	kubectl apply -f k8s/analysis.yaml
	kubectl apply -f k8s/prometheus.yaml

k8s-undeploy:
	kubectl delete -f k8s/ --ignore-not-found

# ── HPA — CPU-based (standard Kubernetes, requires metrics-server) ─────────────
# For minikube: 'minikube addons enable metrics-server' must be run first.
# For k3s:      metrics-server is bundled — no extra step.
k8s-hpa-setup:
	minikube addons enable metrics-server
	kubectl apply -f k8s/hpa.yaml
	@echo "HPA applied. Watch: kubectl get hpa collector-hpa -w"

# ── HPA — CPU + queue-depth (KEDA) ────────────────────────────────────────────
# Installs KEDA cluster-wide, then applies the ScaledObject.
# Removes the plain HPA first (KEDA creates its own HPA object).
k8s-keda-setup:
	minikube addons enable metrics-server
	kubectl apply -f \
	  https://github.com/kedacore/keda/releases/download/v2.15.0/keda-2.15.0.yaml
	kubectl -n keda wait --for=condition=ready pod \
	  -l app=keda-operator --timeout=180s
	-kubectl delete -f k8s/hpa.yaml --ignore-not-found
	kubectl apply -f k8s/keda-scaledobject.yaml
	@echo "KEDA ScaledObject applied."
	@echo "Watch: kubectl get scaledobject collector-scaledobject"
	@echo "       kubectl get hpa"

# ── Observability & status ────────────────────────────────────────────────────

k8s-hpa-status:
	@echo "=== HPA / ScaledObjects ==="
	kubectl get hpa,scaledobject -o wide 2>/dev/null || true
	@echo ""
	@echo "=== Collector pod resource usage ==="
	kubectl top pods -l app=collector 2>/dev/null || \
	  echo "(metrics-server not ready yet)"
	@echo ""
	@echo "=== Collector pods ==="
	kubectl get pods -l app=collector -o wide

# Simulate load: shrink fetch interval to generate bursts.
k8s-load-test:
	kubectl set env deployment/collector FETCH_INTERVAL=10s WINDOW_MAX_SIZE=50
	@echo "Load test started — fetch every 10s, flush every 50 records."
	@echo "Run 'make k8s-hpa-status' to watch HPA scale up."
	@echo "Restore: kubectl set env deployment/collector FETCH_INTERVAL=5m WINDOW_MAX_SIZE=500"

# ── Benchmark: Go vs Python ───────────────────────────────────────────────────

# Python-only benchmark (no running Go instance needed).
bench-python:
	pip install -q aiohttp pyarrow nats-py psutil
	python -m benchmark.runner --lang python \
	  --countries US,GB,DE,FR,PL,IN,AU,CA,BR,JP \
	  --cycles 3

# Compare both (Go collector must be accessible at localhost:8081).
bench-compare:
	pip install -q aiohttp pyarrow nats-py psutil requests
	python -m benchmark.runner --lang both \
	  --go-url http://localhost:8081/metrics \
	  --countries US,GB,DE,FR,PL,IN,AU,CA,BR,JP \
	  --cycles 3

# Shortcut: python-only benchmark then generate report.
bench: bench-python bench-report

# Generate HTML report from the latest results JSON.
bench-report:
	pip install -q plotly
	python -m benchmark.report
	@echo "Open: benchmark/results/*.html"

# ── Cleanup ───────────────────────────────────────────────────────────────────

clean:
	docker compose down -v
	rm -rf collector/go.sum
	rm -f collector/internal/validator/libair_quality_validator.a
	rm -f collector/internal/validator/validator.h
