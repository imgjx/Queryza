#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "[1/3] 编译 C++ 算法库"
(cd algo && g++ -O3 -std=c++17 -fPIC -shared -o libqzalgo.so qzalgo.cpp)

echo "[2/3] 编译 Go 主程序"
GOTOOLCHAIN=local go build -o bin/queryza ./cmd/queryza

echo "[3/3] 启动"
exec ./bin/queryza -config configs/config.yaml "$@"
