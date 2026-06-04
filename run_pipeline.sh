#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <top_callers|tower_heatmap|anomalous_calls|revenue_recon>" >&2
  exit 1
fi

case "$1" in
  top_callers)
    DAG_ID="top_callers_by_spend_dag"
    ;;
  tower_heatmap)
    DAG_ID="tower_utilization_heatmap_dag"
    ;;
  anomalous_calls)
    DAG_ID="anomalous_call_detection_dag"
    ;;
  revenue_recon)
    DAG_ID="revenue_reconciliation_dag"
    ;;
  *)
    echo "Unsupported logical query name: $1" >&2
    exit 1
    ;;
esac

RUN_ID="$(date +%Y%m%d_%H%M%S)"
airflow dags trigger -r "$RUN_ID" --conf "{\"run_id\":\"$RUN_ID\"}" "$DAG_ID"