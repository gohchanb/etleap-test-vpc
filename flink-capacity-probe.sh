#!/usr/bin/env bash
#
# flink-capacity-probe.sh
#   Stage 1: one capacity-snapshot CSV row (run at each job count while ramping).
#   Stage 2: top-K offenders, splitting checkpoint e2e into start_delay (TM-side)
#            vs finalize_residual (JM-side) so you can attribute degradation.
#
# Usage:
#   export BASE="http://ip-10-0-12-56.ec2.internal:20888/proxy/application_1778151203458_5268712"
#   ./flink-capacity-probe.sh                 # prints CSV row + offender table
#   ./flink-capacity-probe.sh >> capacity.csv # append row; offender table goes to stderr
#
# Env knobs:
#   PAR=32        parallel curls
#   TM_SAMPLE=12  how many TMs to sample for avg CPU
#   TOPK=25       offenders to drill into
#   NO_HEADER=1   suppress CSV header (set after first append)
#
set -uo pipefail

BASE="${BASE:?set BASE to the YARN proxy URL of the Flink cluster}"
PAR="${PAR:-32}"
TM_SAMPLE="${TM_SAMPLE:-12}"
TOPK="${TOPK:-25}"

command -v jq >/dev/null || { echo "need jq" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Stage 1: capacity snapshot
# ---------------------------------------------------------------------------

export BASE   # needed by the exported helper functions called via xargs bash -c

ov=$(curl -sS "$BASE/overview")
jobs=$(jq -r '."jobs-running"' <<<"$ov")
slots_total=$(jq -r '."slots-total"' <<<"$ov")
slots_avail=$(jq -r '."slots-available"' <<<"$ov")
tms=$(jq -r '.taskmanagers' <<<"$ov")
slot_util=$(awk "BEGIN{if($slots_total>0)printf \"%.3f\",($slots_total-$slots_avail)/$slots_total; else print 0}")

# Self-labeling run banner so a combined multi-run log stays navigable.
# Printed to stdout AND stderr so it appears in the file regardless of which
# stream you're capturing; starts with ===== so it greps out of CSV cleanly.
banner="===== RUN: $jobs jobs $(date) ====="
echo "$banner"
echo "$banner" >&2

jm=$(curl -sS "$BASE/jobmanager/metrics?get=Status.JVM.CPU.Load,Status.JVM.Memory.Heap.Used,Status.JVM.Memory.Heap.Max,Status.JVM.Threads.Count")
jm_cpu=$(jq -r '.[]|select(.id|endswith("CPU.Load")).value' <<<"$jm")
jm_heap=$(jq -r '.[]|select(.id|endswith("Heap.Used")).value' <<<"$jm")
jm_heapmax=$(jq -r '.[]|select(.id|endswith("Heap.Max")).value' <<<"$jm")
jm_threads=$(jq -r '.[]|select(.id|endswith("Threads.Count")).value' <<<"$jm")
jm_heap_pct=$(awk "BEGIN{if($jm_heapmax>0)printf \"%.2f\",$jm_heap/$jm_heapmax; else print 0}")

# IMPORTANT: never pipe `xargs -P curl | jq`. Parallel curls interleave their
# bytes into the single pipe, jq hits a mangled doc and exits, and the still-
# running curls then die with "(23) Failure writing output to destination".
# Each parallel worker must parse its OWN response and emit one clean line.

# One TM -> its CPU load, one line.
tm_cpu_one() { curl -sS "$1/taskmanagers/$2/metrics?get=Status.JVM.CPU.Load" \
  | jq -r '.[0].value // empty'; }
export -f tm_cpu_one

tm_cpu=$(curl -sS "$BASE/taskmanagers" | jq -r '.taskmanagers[].id' | head -"$TM_SAMPLE" \
  | xargs -P"$PAR" -I{} bash -c 'tm_cpu_one "$BASE" "$@"' _ {} \
  | awk '{s+=$1;n++} END{if(n)printf "%.3f",s/n; else print 0}')

# Per-job checkpoint summary sweep: e2e avg + failed counts, in one pass.
# Cache the running job ids once.
JOBS=$(curl -sS "$BASE/jobs" | jq -r '.jobs[]|select(.status=="RUNNING").id')

# One job -> "e2e_avg failed", one line.
summ_one() { curl -sS "$1/jobs/$2/checkpoints" \
  | jq -r '"\(.summary.end_to_end_duration.avg // 0) \(.counts.failed // 0)"'; }
export -f summ_one

summ=$(printf '%s\n' "$JOBS" \
  | xargs -P"$PAR" -I{} bash -c 'summ_one "$BASE" "$@"' _ {})

e2e_sorted=$(awk '{print $1}' <<<"$summ" | sort -n)
ckfail=$(awk '{s+=$2} END{print s+0}' <<<"$summ")

read -r p50 p90 p99 emax <<<"$(awk '{a[NR]=$1} END{
  if(NR==0){print "0 0 0 0";exit}
  printf "%d %d %d %d", a[int(NR*0.50)+0], a[int(NR*0.90)+0], a[int(NR*0.99)+0], a[NR]
}' <<<"$e2e_sorted")"

ts=$(date +%s)
[ -z "${NO_HEADER:-}" ] && \
  echo "ts,jobs,tms,slot_util,jm_cpu,jm_heap_pct,jm_threads,tm_cpu_avg,ck_e2e_p50_ms,ck_e2e_p90_ms,ck_e2e_p99_ms,ck_e2e_max_ms,ck_failed_total"
echo "$ts,$jobs,$tms,$slot_util,$jm_cpu,$jm_heap_pct,$jm_threads,$tm_cpu,$p50,$p90,$p99,$emax,$ckfail"

# ---------------------------------------------------------------------------
# Stage 2: top-K offenders — start_delay (TM/backpressure) vs
#          finalize_residual = e2e - start_delay (JM/S3 finalize)
# Goes to stderr so Stage 1 stays a clean CSV when redirected.
# ---------------------------------------------------------------------------

# Drill one job: echo "e2e start_delay job_id" or nothing on failure.
offender_probe() {
  local base="$1" j="$2"
  local ck details ckid vid sd e2e
  ck=$(curl -sS "$base/jobs/$j/checkpoints" | jq -r '.latest.completed.id // empty')
  [ -z "$ck" ] && return 0
  details=$(curl -sS "$base/jobs/$j/checkpoints/details/$ck")
  e2e=$(jq -r '.end_to_end_duration // empty' <<<"$details")
  ckid=$(jq -r '.id // empty' <<<"$details")
  vid=$(jq -r '.tasks|keys[0] // empty' <<<"$details")
  { [ -z "$e2e" ] || [ -z "$vid" ] || [ -z "$ckid" ]; } && return 0
  sd=$(curl -sS "$base/jobs/$j/checkpoints/details/$ckid/subtasks/$vid" \
       | jq -r '.summary.start_delay.max // 0')
  echo "$e2e $sd $j"
}
export -f offender_probe

# jid -> job name, one call for the whole cluster (TAB-separated; names may have spaces).
names=$(curl -sS "$BASE/jobs/overview" | jq -r '.jobs[] | "\(.jid)\t\(.name)"')

offenders=$(printf '%s\n' "$JOBS" \
  | xargs -P"$PAR" -I{} bash -c 'offender_probe "$BASE" "$@"' _ {} \
  | sort -rn | head -"$TOPK")

{
  echo
  echo "# top-$TOPK offenders: start_delay vs finalize_residual (e2e - start_delay)"
  # First file = name map (TAB-sep); second = offender lines (space-sep "e2e sd jid").
  awk '
    FNR==NR { t=index($0,"\t"); name[substr($0,1,t-1)]=substr($0,t+1); next }
    {
      e=$1; sd=$2; jid=$3; sd_pct=(e>0)?100*sd/e:0
      nm=(jid in name)?name[jid]:"?"
      printf "e2e=%-7d start_delay=%-7d finalize_residual=%-7d sd%%=%-5.1f %-34s %s\n", \
             e, sd, e-sd, sd_pct, jid, nm
    }' <(printf '%s\n' "$names") <(printf '%s\n' "$offenders")
  echo "# read: sd% high with idle TMs -> JM trigger-path bound; finalize_residual high -> JM finalize/S3 bound"
} >&2
