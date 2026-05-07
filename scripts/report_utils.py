import re
from pathlib import Path
import statistics
import difflib


def _extract_metric(pattern, text, is_float=False):
    match = re.search(pattern, text)
    if match:
        val = match.group(1)
        return float(val) if is_float else int(val)
    return None


def create_aggregate_report(query_name, tag, populations, output_paths, timestamp):
    report_dir = Path("reports")
    report_dir.mkdir(parents=True, exist_ok=True)

    total_durations = []
    costs = []
    plan_text = "N/A"

    cost_pattern = r"(?:Cost\s*[:=]?\s*|cost=)(\d+)"

    for i, path in enumerate(output_paths):
        content = path.read_text(encoding="utf-8")

        duration = _extract_metric(
            r"Total Duration \(s\):\s*([\d.]+)", content, is_float=True
        )
        if duration is not None:
            total_durations.append(duration)

        cost_match = re.search(
            r"\|\s*0\s*\|\s*(?:SELECT|INSERT|UPDATE|DELETE)\s+STATEMENT\s*\|.*?\|\s*(\d+[KMG]?)\s*\(",
            content,
            re.IGNORECASE,
        )
        if cost_match:
            cost_str = cost_match.group(1).upper()
            multiplier = 1
            if "K" in cost_str:
                multiplier = 1000
                cost_str = cost_str.replace("K", "")
            elif "M" in cost_str:
                multiplier = 1000000
                cost_str = cost_str.replace("M", "")
            elif "G" in cost_str:
                multiplier = 1000000000
                cost_str = cost_str.replace("G", "")
            try:
                costs.append(float(cost_str) * multiplier)
            except ValueError:
                pass

        if i == 0:
            plan_split = content.split("-- Estimated Execution Plan --")
            if len(plan_split) > 1:
                plan_text = plan_split[1].strip()

    avg_duration = sum(total_durations) / len(total_durations) if total_durations else 0
    min_duration = min(total_durations) if total_durations else 0
    max_duration = max(total_durations) if total_durations else 0

    avg_cost = sum(costs) / len(costs) if costs else 0
    var_duration = (
        statistics.variance(total_durations) if len(total_durations) > 1 else 0
    )

    report_lines = [
        f"=== AGGREGATE REPORT: {query_name} ===",
        f"Tag: {tag}",
        f"Populations: {populations}",
        f"Avg Total Duration (s): {avg_duration:.4f}",
        f"Min Duration (s): {min_duration:.4f}",
        f"Max Duration (s): {max_duration:.4f}",
        f"Variance Duration: {var_duration:.6f}",
        f"Avg Cost: {avg_cost:.2f}",
        "",
        "--- Individual Run Files ---",
    ]
    for p in output_paths:
        report_lines.append(f"- {p.name}")

    report_lines.append("")
    report_lines.append("-- Estimated Execution Plan --")
    report_lines.append(plan_text)

    report_path = report_dir / f"{query_name}_{tag}_aggregate_{timestamp}.txt"
    report_path.write_text("\n".join(report_lines), encoding="utf-8")

    return report_path


def generate_comparison_table(tag1, tag2):
    base_dir = Path("reports")
    if not base_dir.exists():
        print("No reports found.")
        return

    tag1_files = list(base_dir.glob(f"*_{tag1}_aggregate_*.txt"))
    tag2_files = list(base_dir.glob(f"*_{tag2}_aggregate_*.txt"))

    queries = set()
    data = {}

    def parse_report(p):
        content = p.read_text(encoding="utf-8")
        dur = _extract_metric(r"Avg Total Duration \(s\):\s*([\d.]+)", content, True)
        min_dur = _extract_metric(r"Min Duration \(s\):\s*([\d.]+)", content, True)
        max_dur = _extract_metric(r"Max Duration \(s\):\s*([\d.]+)", content, True)
        cost = _extract_metric(r"Avg Cost:\s*([\d.]+)", content, True)

        plan_text = ""
        plan_split = content.split("-- Estimated Execution Plan --")
        if len(plan_split) > 1:
            plan_text = plan_split[1].strip()

            # Normalize Oracle system generated temporary table names
            # SYS_TEMP_0FD9D6631_7811F0 -> SYS_TEMP_...
            plan_text = re.sub(r"SYS_TEMP_[A-F0-9_]+", "SYS_TEMP_XXX", plan_text)

        match = re.match(r"(query\d+)_", p.name)
        qname = match.group(1) if match else "unknown"
        val = {
            "dur": dur or 0.0,
            "min_dur": min_dur or 0.0,
            "max_dur": max_dur or 0.0,
            "cost": cost or 0.0,
            "plan": plan_text,
            "path": p.name,
        }
        return qname, val

    for f in tag1_files:
        qn, v = parse_report(f)
        queries.add(qn)
        if qn not in data:
            data[qn] = {}
        data[qn][tag1] = v

    for f in tag2_files:
        qn, v = parse_report(f)
        queries.add(qn)
        if qn not in data:
            data[qn] = {}
        data[qn][tag2] = v

    report_str = []

    def log_and_print(msg):
        print(msg)
        report_str.append(msg)

    log_and_print(f"\n{'='*115}")
    log_and_print(f"{'COMPARISON REPORT':^115}")
    log_and_print(f"{'='*115}")
    log_and_print(
        f"{'Query':<10} | {'Metric':<15} | {tag1:<15} | {tag2:<15} | {'Diff (%)':<10} | {'Status':<15} | {'Plan Changed':<12}"
    )
    log_and_print("-" * 115)

    for q in sorted(queries):
        d1 = data.get(q, {}).get(
            tag1, {"dur": 0.0, "min_dur": 0.0, "max_dur": 0.0, "cost": 0.0, "plan": ""}
        )
        d2 = data.get(q, {}).get(
            tag2, {"dur": 0.0, "min_dur": 0.0, "max_dur": 0.0, "cost": 0.0, "plan": ""}
        )

        plan_changed = (
            "YES" if d1["plan"] != d2["plan"] and d1["plan"] and d2["plan"] else "NO"
        )
        if not d1["plan"] or not d2["plan"]:
            plan_changed = "N/A"

        # Duration
        dur_diff = 0
        dur_status = "N/A"
        if d1["dur"] > 0:
            dur_diff = ((d2["dur"] - d1["dur"]) / d1["dur"]) * 100
            if dur_diff < -1:
                dur_status = "IMPROVED"
            elif dur_diff > 1:
                dur_status = "DEGRADED"
            else:
                dur_status = "SAME"

        log_and_print(
            f"{q:<10} | {'Duration (Avg)':<15} | {d1['dur']:<15.4f} | {d2['dur']:<15.4f} | {dur_diff:<10.2f} | {dur_status:<15} | {plan_changed:<12}"
        )
        log_and_print(
            f"{' ':<10} | {'Duration (Min)':<15} | {d1['min_dur']:<15.4f} | {d2['min_dur']:<15.4f} | {'-':<10} | {'-':<15} | {'':<12}"
        )
        log_and_print(
            f"{' ':<10} | {'Duration (Max)':<15} | {d1['max_dur']:<15.4f} | {d2['max_dur']:<15.4f} | {'-':<10} | {'-':<15} | {'':<12}"
        )

        # Cost
        cost_diff = 0
        cost_status = "N/A"
        if d1["cost"] > 0:
            cost_diff = ((d2["cost"] - d1["cost"]) / d1["cost"]) * 100
            if cost_diff < -1:
                cost_status = "IMPROVED"
            elif cost_diff > 1:
                cost_status = "DEGRADED"
            else:
                cost_status = "SAME"

        log_and_print(
            f"{' ':<10} | {'Cost':<15} | {d1['cost']:<15.2f} | {d2['cost']:<15.2f} | {cost_diff:<10.2f} | {cost_status:<15} | {'':<12}"
        )
        log_and_print("-" * 115)

        if plan_changed == "YES":
            diff = list(
                difflib.unified_diff(
                    d1["plan"].splitlines(),
                    d2["plan"].splitlines(),
                    fromfile=f"{tag1} Plan",
                    tofile=f"{tag2} Plan",
                    lineterm="",
                )
            )
            log_and_print(f"--- QUERY PLAN DIFF FOR {q} ---")
            for line in diff:
                log_and_print(line)
            log_and_print("-" * 115)

    log_and_print(f"{'='*115}\n")

    import datetime

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = base_dir / f"compare_{tag1}_vs_{tag2}_{timestamp}.txt"
    out_path.write_text("\n".join(report_str), encoding="utf-8")
    print(f"Saved comparison report to {out_path}")
