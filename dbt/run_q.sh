#!/bin/zsh
# usage: ./run_q.sh file.sql [limit]
cd "$(dirname "$0")"
dbt show --profiles-dir . --inline "$(cat "$1")" --limit "${2:-100}" 2>&1 | sed -n '/Previewing inline node/,$p'
