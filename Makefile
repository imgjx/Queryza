CXX ?= g++
CXXFLAGS ?= -O3 -std=c++17 -fPIC
GO ?= go
VERSION ?= 0.1.0

.PHONY: all algo build run clean deps dist

all: build

# 算法层编为静态库，最终二进制自包含，分发时不需要附带 .so
algo:
	cd algo && $(CXX) $(CXXFLAGS) -c qzalgo.cpp -o qzalgo.o
	cd algo && ar rcs libqzalgo.a qzalgo.o
	@echo "built algo/libqzalgo.a"

build: algo
	GOTOOLCHAIN=local $(GO) build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o bin/queryza ./cmd/queryza
	@echo "built bin/queryza ($(VERSION))"

run: build
	./bin/queryza -config configs/config.yaml

deps:
	@echo "需要: Go 1.21+, g++, MySQL/MariaDB, Redis"

# 构建发布包（默认 linux/amd64）
dist: build
	@bash scripts/dist.sh $(VERSION) $$(go env GOARCH)

clean:
	rm -rf bin dist algo/libqzalgo.so algo/libqzalgo.a algo/qzalgo.o
