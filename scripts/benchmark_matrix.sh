#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-127.0.0.1}"
THIS_PORT="${THIS_PORT:-6380}"
REDIS_PORT="${REDIS_PORT:-6381}"
REQUESTS="${REQUESTS:-1000000}"
CLIENTS="${CLIENTS:-50}"
PIPELINE="${PIPELINE:-16}"
THREADS="${THREADS:-4}"
PAYLOAD_SIZE="${PAYLOAD_SIZE:-1024}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
RESULTS="$TMP_DIR/results.tsv"
THIS_PID=""
REDIS_PID=""

cleanup() {
    if [[ -n "$THIS_PID" ]]; then
        kill "$THIS_PID" >/dev/null 2>&1 || true
        wait "$THIS_PID" >/dev/null 2>&1 || true
    fi

    if [[ -n "$REDIS_PID" ]]; then
        kill "$REDIS_PID" >/dev/null 2>&1 || true
        wait "$REDIS_PID" >/dev/null 2>&1 || true
    fi

    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

need_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required command: $1" >&2
        exit 1
    fi
}

check_port_free() {
    local port="$1"
    if lsof -tiTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "port $port is already in use" >&2
        exit 1
    fi
}

wait_for_ping() {
    local port="$1"
    local name="$2"

    for _ in {1..50}; do
        if redis-cli -h "$HOST" -p "$port" PING >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.1
    done

    echo "$name did not become ready on $HOST:$port" >&2
    exit 1
}

record_result() {
    local mode="$1"
    local server="$2"
    local command="$3"
    local output="$4"
    local rps

    rps="$(printf '%s\n' "$output" | awk -F',' '/^"/ && $1 != "\"test\"" { gsub(/"/, "", $2); print $2; exit }')"

    if [[ -z "$rps" ]]; then
        echo "could not parse redis-benchmark output for $mode / $server / $command" >&2
        exit 1
    fi

    printf '%s\t%s\t%s\t%s\n' "$mode" "$server" "$command" "$rps" >> "$RESULTS"
}

run_benchmark() {
    local mode="$1"
    local server="$2"
    local command="$3"
    shift 3

    echo
    echo "### $mode / $server / $command"

    local output
    output="$("$@" 2>&1)"
    printf '%s\n' "$output"
    record_result "$mode" "$server" "$command" "$output"
}

seed_value() {
    local port="$1"
    redis-cli -h "$HOST" -p "$port" SET bench:key value >/dev/null
}

seed_payload_value() {
    local port="$1"
    perl -e "print 'x' x $PAYLOAD_SIZE" | redis-cli -h "$HOST" -p "$port" -x SET bench:key >/dev/null
}

run_standard_matrix_for_server() {
    local mode="$1"
    local threads="$2"
    local server="$3"
    local port="$4"
    local args=()

    if [[ "$threads" -gt 1 ]]; then
        args+=(--threads "$threads")
    fi
    args+=(-h "$HOST" -p "$port" -n "$REQUESTS" -c "$CLIENTS" -P "$PIPELINE" --csv)

    echo
    echo "## $mode / $server ($HOST:$port)"

    seed_value "$port"
    run_benchmark "$mode" "$server" "PING" redis-benchmark "${args[@]}" PING
    run_benchmark "$mode" "$server" "ECHO" redis-benchmark "${args[@]}" ECHO value
    run_benchmark "$mode" "$server" "SET" redis-benchmark "${args[@]}" SET bench:key value
    run_benchmark "$mode" "$server" "SET EX" redis-benchmark "${args[@]}" SET bench:key value EX 60

    seed_value "$port"
    run_benchmark "$mode" "$server" "GET" redis-benchmark "${args[@]}" GET bench:key

    seed_value "$port"
    run_benchmark "$mode" "$server" "DEL" redis-benchmark "${args[@]}" DEL bench:key

    seed_value "$port"
    run_benchmark "$mode" "$server" "EXISTS" redis-benchmark "${args[@]}" EXISTS bench:key

    seed_value "$port"
    run_benchmark "$mode" "$server" "EXPIRE" redis-benchmark "${args[@]}" EXPIRE bench:key 60

    redis-cli -h "$HOST" -p "$port" SET bench:key value EX 60 >/dev/null
    run_benchmark "$mode" "$server" "TTL" redis-benchmark "${args[@]}" TTL bench:key

    run_benchmark "$mode" "$server" "INFO" redis-benchmark "${args[@]}" INFO
}

run_payload_matrix_for_server() {
    local mode="$1"
    local server="$2"
    local port="$3"
    local args=()

    if [[ "$THREADS" -gt 1 ]]; then
        args+=(--threads "$THREADS")
    fi
    args+=(-h "$HOST" -p "$port" -n "$REQUESTS" -c "$CLIENTS" -P "$PIPELINE" --csv)

    echo
    echo "## $mode / $server ($HOST:$port)"

    echo
    echo "### $mode / $server / ECHO"
    local echo_output
    echo_output="$(perl -e "print 'x' x $PAYLOAD_SIZE" | redis-benchmark "${args[@]}" -x ECHO 2>&1)"
    printf '%s\n' "$echo_output"
    record_result "$mode" "$server" "ECHO" "$echo_output"

    echo
    echo "### $mode / $server / SET"
    local set_output
    set_output="$(perl -e "print 'x' x $PAYLOAD_SIZE" | redis-benchmark "${args[@]}" -x SET bench:key 2>&1)"
    printf '%s\n' "$set_output"
    record_result "$mode" "$server" "SET" "$set_output"

    seed_payload_value "$port"
    run_benchmark "$mode" "$server" "GET" redis-benchmark "${args[@]}" GET bench:key
}

lookup_rps() {
    local mode="$1"
    local server="$2"
    local command="$3"
    awk -F '\t' -v mode="$mode" -v server="$server" -v command="$command" \
        '$1 == mode && $2 == server && $3 == command { print $4; exit }' "$RESULTS"
}

ratio() {
    local this_rps="$1"
    local redis_rps="$2"
    awk -v this_rps="$this_rps" -v redis_rps="$redis_rps" \
        'BEGIN { if (redis_rps > 0) printf "%.2f", this_rps / redis_rps; else printf "n/a" }'
}

print_table() {
    local mode="$1"
    shift

    echo
    echo "## Summary: $mode"
    echo
    echo "| Command | This server req/s | redis-server req/s | This / Redis |"
    echo "| --- | ---: | ---: | ---: |"

    local command
    for command in "$@"; do
        local this_rps
        local redis_rps
        this_rps="$(lookup_rps "$mode" "this-server" "$command")"
        redis_rps="$(lookup_rps "$mode" "redis-server" "$command")"
        printf '| `%s` | %.0f | %.0f | %sx |\n' \
            "$command" \
            "$this_rps" \
            "$redis_rps" \
            "$(ratio "$this_rps" "$redis_rps")"
    done
}

print_average() {
    local mode="$1"

    awk -F '\t' -v mode="$mode" '
        $1 == mode && $3 != "INFO" && $2 == "this-server" {
            this_sum += $4
            this_count += 1
        }
        $1 == mode && $3 != "INFO" && $2 == "redis-server" {
            redis_sum += $4
            redis_count += 1
        }
        END {
            if (this_count > 0 && redis_count > 0) {
                this_avg = this_sum / this_count
                redis_avg = redis_sum / redis_count
                printf "%s average: this-server %.0f req/s, redis-server %.0f req/s, ratio %.2fx\n", mode, this_avg, redis_avg, this_avg / redis_avg
            }
        }
    ' "$RESULTS"
}

cd "$ROOT_DIR"

need_command cargo
need_command redis-server
need_command redis-benchmark
need_command redis-cli
need_command perl
need_command awk
need_command lsof

check_port_free "$THIS_PORT"
check_port_free "$REDIS_PORT"

echo "Building release binary..."
cargo build --release

echo "Starting this server on $HOST:$THIS_PORT..."
target/release/redis --port "$THIS_PORT" --host "$HOST" > "$TMP_DIR/this-server.log" 2>&1 &
THIS_PID="$!"
wait_for_ping "$THIS_PORT" "this server"

echo "Starting redis-server on $HOST:$REDIS_PORT..."
redis-server --port "$REDIS_PORT" --bind "$HOST" --save "" --appendonly no > "$TMP_DIR/redis-server.log" 2>&1 &
REDIS_PID="$!"
wait_for_ping "$REDIS_PORT" "redis-server"

: > "$RESULTS"

run_standard_matrix_for_server "P16" 1 "this-server" "$THIS_PORT"
run_standard_matrix_for_server "P16" 1 "redis-server" "$REDIS_PORT"

run_standard_matrix_for_server "P16 T4" "$THREADS" "this-server" "$THIS_PORT"
run_standard_matrix_for_server "P16 T4" "$THREADS" "redis-server" "$REDIS_PORT"

run_payload_matrix_for_server "P16 T4 ${PAYLOAD_SIZE}B" "this-server" "$THIS_PORT"
run_payload_matrix_for_server "P16 T4 ${PAYLOAD_SIZE}B" "redis-server" "$REDIS_PORT"

echo
echo "# Benchmark Summary"

standard_commands=(PING ECHO SET "SET EX" GET DEL EXISTS EXPIRE TTL)
payload_commands=(ECHO SET GET)

print_table "P16" "${standard_commands[@]}"
print_average "P16"

print_table "P16 T4" "${standard_commands[@]}"
print_average "P16 T4"

print_table "P16 T4 ${PAYLOAD_SIZE}B" "${payload_commands[@]}"
print_average "P16 T4 ${PAYLOAD_SIZE}B"

echo
echo "Raw result rows:"
echo "mode	server	command	rps"
cat "$RESULTS"
