# shellcheck shell=bash
# Helpers the suite scripts share: building the loader for the Docker VM, and
# bringing a cluster up. Source it from the repository root.

case $(docker info --format '{{.Architecture}}') in
  aarch64 | arm64) goarch=arm64 ;;
  *) goarch=amd64 ;;
esac
build() { CGO_ENABLED=0 GOOS=linux GOARCH=$goarch go build "$@"; }

# workspace writes a go.work that builds this module against the opensearch-go
# checkout $1 and prints its path. The go.work lives in a temporary directory,
# so $1 is made absolute first.
workspace() {
  local work osgo
  osgo=$(cd "$1" && pwd)
  work=$(mktemp -d)/go.work
  printf 'go %s\n\nuse (\n\t"%s"\n\t"%s"\n)\n' "$(go env GOVERSION | sed 's/^go//')" "$PWD" "$osgo" >"$work"
  echo "$work"
}

say() { echo "[$(date +%H:%M:%S)] $*"; }

wait_for() { # $1 = expected nodes
  for _ in $(seq 1 120); do
    if curl -sf "localhost:9200/_cluster/health?wait_for_nodes=$1&wait_for_status=yellow&timeout=5s" >/dev/null; then
      return 0
    fi
    sleep 2
  done
  say "cluster with $1 nodes never became healthy"
  exit 1
}

up() { # $1 = compose file, $2 = nodes, $3 = write queue size
  docker compose -f docker/single-node.yml down -v >/dev/null 2>&1 || true
  docker compose -f docker/cluster.yml down -v >/dev/null 2>&1 || true
  WRITE_QUEUE_SIZE=$3 docker compose -f "$1" up -d >/dev/null
  wait_for "$2"
  say "up: $1, $2 node(s), write queue $3, $(curl -s 'localhost:9200/_cat/thread_pool/write?h=size,queue_size' | head -1)"
}
