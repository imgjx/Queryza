#!/usr/bin/env bash
# 构建 Linux 发布包：dist/queryza-<version>-linux-<arch>.tar.gz
set -euo pipefail

VERSION="${1:-0.1.0}"
ARCH="${2:-amd64}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

NAME="queryza-${VERSION}-linux-${ARCH}"
OUT="dist/${NAME}"

# 用目标版本重新编译，避免复用带旧版本号的二进制
make -s algo >/dev/null
GOTOOLCHAIN=local go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o bin/queryza ./cmd/queryza

rm -rf "$OUT"
mkdir -p "$OUT/configs" "$OUT/seeds"

cp bin/queryza          "$OUT/queryza"
cp configs/config.yaml  "$OUT/configs/config.yaml"
cp seeds/seeds.json     "$OUT/seeds/seeds.json"
cp README.md            "$OUT/README.md"
[ -f LICENSE ] && cp LICENSE "$OUT/LICENSE"
[ -f CHANGELOG.md ] && cp CHANGELOG.md "$OUT/CHANGELOG.md"

cat > "$OUT/config.example.env" <<'EOF'
# 可直接覆盖的运行时环境变量（启动脚本会读取）
QZ_ADMIN_USER=admin
QZ_ADMIN_PASSWORD=admin123
QZ_ADMIN_PATH=/admin
EOF

cat > "$OUT/start.sh" <<'EOF'
#!/usr/bin/env bash
# Queryza 启动脚本：同目录下可直接运行，无需额外依赖
set -euo pipefail
cd "$(dirname "$0")"

CONFIG="${QZ_CONFIG:-configs/config.yaml}"
ADMIN_PATH="${QZ_ADMIN_PATH:-}"

ARGS=(-config "$CONFIG")
[ -n "$ADMIN_PATH" ] && ARGS+=(-admin-path "$ADMIN_PATH")

exec ./queryza "${ARGS[@]}"
EOF
chmod +x "$OUT/queryza" "$OUT/start.sh"

cat > "$OUT/queryza.service" <<'EOF'
[Unit]
Description=Queryza Search Engine
After=network.target mysql.service redis.service

[Service]
Type=simple
User=queryza
WorkingDirectory=/opt/queryza
ExecStart=/opt/queryza/queryza -config /opt/queryza/configs/config.yaml
Restart=on-failure
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

echo "== 检查二进制依赖 =="
if ldd "$OUT/queryza" 2>/dev/null | grep -qi "libqzalgo"; then
  echo "ERROR: 二进制仍依赖 libqzalgo.so，发布包不可移植" >&2
  exit 1
fi

tar -C dist -czf "dist/${NAME}.tar.gz" "$NAME"

if command -v sha256sum >/dev/null 2>&1; then
  ( cd dist && sha256sum "${NAME}.tar.gz" > "${NAME}.tar.gz.sha256" )
fi

echo "== 产物 =="
ls -lh "dist/${NAME}.tar.gz" "dist/${NAME}.tar.gz.sha256" 2>/dev/null || true
