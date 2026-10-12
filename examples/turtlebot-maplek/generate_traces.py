"""Generate small synthetic observation journals for the MAPLE-K tutorial.

These teaching traces use the native reference journal and separate immutable
Knowledge commits. The archived inline journals remain unchanged. Each native
manifest records exact extent, byte digest, clock label and semantic-event to
physical-record mapping. Run from any directory; output stays beside this script.
"""

import hashlib
import json
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent
EPOCH = "tutorial-epoch"
CLOCK = "turtlebot.journal"
LOOP = ["turtlebot4", "lidar_occlusion"]


def tagged(name, payload=None):
    result = {"$tag": name}
    if payload is not None:
        result["payload"] = payload
    return result


def snake(name):
    return "".join(("_" if i else "") + c.lower() if c.isupper() else c
                   for i, c in enumerate(name))


def trace(scenario):
    rows = []
    clock = 0.0
    sent = {}

    def emit(tag, payload):
        nonlocal clock
        rows.append({"schema_version": 4, "position": {"epoch": EPOCH, "seq": len(rows) + 1},
                     "arrival": {"journal_time": round(clock, 2), "journal_clock": CLOCK},
                     "$tag": tag, "payload": payload})
        clock += 0.1

    def resolved(key, value, writer, seq):
        return {"ref": {"store_epoch": EPOCH, "global_write_sequence": seq, "key": key},
                "value": value, "written_by": writer}

    def phase(a, step, evidence=None):
        emit("Phase", {"activation": a["record"], "step": step, "evidence": evidence or {}})

    def activation(role, producer, cause=None):
        return {"record": {"epoch": EPOCH, "start_seq": None, "producer": producer,
                           "role": role, "loop": LOOP, "caused_by": cause},
                "emitted": 0}

    def start(a, trigger, kind="event"):
        # An activation's identity is the position of its Start; a woken
        # callback receives exactly the message its publication sent.
        a["record"]["start_seq"] = len(rows) + 1
        topic = "scan" if trigger == "LidarData" else snake(trigger)
        cause = a["record"]["caused_by"]
        message = sent.get(json.dumps(cause, sort_keys=True), {}) if cause else {}
        phase(a, tagged("Start", {"kind": kind, "topic": topic, "message": message}))

    def write(a, key, value):
        seq = len(rows) + 1
        identity = {"epoch": EPOCH, "start_seq": a["record"]["start_seq"]}
        version = resolved(key, value, tagged("Activation", identity), seq)
        phase(a, tagged("Write", {"key": key, "value": version}))
        return version

    def external(key, value, name):
        seq = len(rows) + 1
        version = resolved(key, value, tagged("Node", {"name": name}), seq)
        emit("Knowledge", {"version": version})
        return version

    def read(a, version):
        phase(a, tagged("Read", {"key": version["ref"]["key"], "value": version}))

    def event(a, name):
        a["emitted"] += 1
        emission = {"activation": {"epoch": EPOCH, "start_seq": a["record"]["start_seq"]},
                    "sequence_no": a["emitted"]}
        message = {"stage": snake(name)}
        sent[json.dumps(emission, sort_keys=True)] = message
        phase(a, tagged("Event", {"kind": "event", "topic": snake(name),
                                  "sequence_no": a["emitted"], "message": message}))
        return emission

    def end(a, outcome):
        phase(a, tagged("End", {"outcome": outcome}))

    monitor = activation("Monitor", "Monitor")
    start(monitor, "LidarData", kind="message")
    scans = write(monitor, "LidarScans", {"scans": [[1.0, 0.0, 0.0, 2.0]]})
    wake = event(monitor, "ProcessedData")
    end(monitor, tagged("Monitored", {"observation": scans}))
    analysis = activation("Analysis", "Analysis", wake)
    start(analysis, "ProcessedData")
    read(analysis, scans)
    older_masks = write(analysis, "BoolLidarMasks",
                        {"masks": [{"values": [False, True, False, False], "baseAngle": 0.0}]})
    masks = write(analysis, "BoolLidarMasks", {"masks": [
        {"values": [False, False, False, False] if scenario in ("false-anomaly", "nominal")
         else [False, True, True, False], "baseAngle": 0.0}]})
    if scenario == "nominal":
        end(analysis, tagged("Analysed", tagged("Nominal")))
        return rows
    wake = event(analysis, "AnomalyFound")
    end(analysis, tagged("Analysed", tagged("Anomaly", {"evidence": masks})))
    if scenario == "slow":
        clock += 3.0
    if scenario == "no-plan":
        planner = activation("Plan", "Plan", wake)
        start(planner, "AnomalyFound")
        end(planner, tagged("Planned", tagged("NoPlan")))
        return rows

    attempts = 3 if scenario == "retry-exhausted" else 2 if scenario == "replanned" else 1
    for attempt in range(attempts):
        planner = activation("Plan", "Plan", wake)
        start(planner, "AnomalyFound" if attempt == 0 else "PlanRejected")
        if scenario != "missing-read":
            read(planner, older_masks if scenario == "stale" else masks)
        commands = [{"angleVelocity": 0.0 if scenario == "ineffective" else
                     0.8 if scenario == "unsafe" or (attempt == 0 and attempts == 2) else 0.4,
                     "duration": 8.0}]
        if scenario == "same-value-update":
            # Another writer stores the same mask values under a newer version.
            external("BoolLidarMasks", masks["value"], "mask-updater")
        if scenario == "external-plan":
            # The plan comes from another controller; Plan only reads it.
            plan = external("Directions", {"commands": commands}, "other-controller")
            read(planner, plan)
        else:
            plan = write(planner, "Directions", {"commands": commands})
        wake = event(planner, "PlanningCompleted")
        if scenario == "withdrawn":
            end(planner, tagged("Aborted", {"reason": tagged("Failed", "timeout")}))
            return rows
        legitimate = activation("Legitimate", "Legitimate", wake)
        if scenario == "overlap":
            start(legitimate, "PlanningCompleted")
        end(planner, tagged("Planned", tagged("Generated", {"plan": plan})))
        if scenario == "unvalidated":
            break
        if scenario != "overlap":
            start(legitimate, "PlanningCompleted")
        read(legitimate, plan)
        accepted = attempt == attempts - 1
        result = write(legitimate, "VerificationInfo", {"accepted": accepted})
        wake = event(legitimate, "PlanAccepted" if accepted else "PlanRejected")
        end(legitimate, tagged("Legitimised", tagged("Accepted" if accepted else "Rejected",
            {"plan": plan, "legitimate_result": result})))

    executor = activation("Execute", "Execute", wake)
    start(executor, "PlanningCompleted" if scenario == "unvalidated" else "PlanAccepted")
    read(executor, plan)
    phase(executor, tagged("ServiceStarted", {"service": "apply_spin_config", "request_id": "req1"}))
    if scenario == "pending-service":
        return rows
    if scenario == "service-conflict":
        phase(executor, tagged("ServiceStarted", {"service": "apply_spin_config", "request_id": "req2"}))
    if scenario != "owner-ended":
        phase(executor, tagged("ServiceCompleted", {"service": "apply_spin_config", "request_id": "req1",
            "outcome": "NotDelivered" if scenario == "not-delivered" else "Delivered"}))
    if scenario == "not-delivered":
        end(executor, tagged("Executed", tagged("NotApplied", {"plan": plan,
            "reason": tagged("Failed", "delivery failed")})))
        return rows
    applied_commands = [{"angleVelocity": 0.2, "duration": 8.0}] if scenario == "wrong-effect" else commands
    effect = write(executor, "SpinConfig", {"commands": applied_commands, "period": 10})
    end(executor, tagged("Executed", tagged("Applied", {"plan": plan, "effect": effect})))
    return rows


def native_trace(scenario):
    """Preserve semantic events, adding immutable commits before their Write facts.

    Archived schema-4 JSONL is retained beside this script. Mapping entries bind
    its original semantic row to an admitted native record, not to a write count.
    """
    archive = ROOT / f"{scenario}.jsonl"
    original = [json.loads(line) for line in archive.read_text().splitlines()]
    if original != trace(scenario):
        raise ValueError(f"archived semantic trace changed: {archive}")
    rows, mapping, versions = [], [], {}
    run = "tutorial-run"
    token = lambda seq: str(uuid.uuid5(uuid.NAMESPACE_URL, f"turtlebot/{scenario}/{seq}"))

    def reference(old):
        return versions[(old["ref"]["key"], old["ref"]["global_write_sequence"])]

    def resolved_refs(value):
        if isinstance(value, dict):
            if "ref" in value and "written_by" in value:
                return reference(value)
            return {k: resolved_refs(v) for k, v in value.items()}
        if isinstance(value, list):
            return [resolved_refs(v) for v in value]
        return value

    def emit(tag, payload, at):
        rows.append({"schema_version": 2, "$tag": tag,
                     "payload": {**payload, "arrival": {"journal_time": at}}})

    def commit(version, at):
        old = version["ref"]
        ref = {"store_epoch": EPOCH, "global_write_sequence": len(versions) + 1,
               "key": old["key"]}
        versions[(old["key"], old["global_write_sequence"])] = ref
        writer = version["written_by"]
        if writer["$tag"] == "Activation":
            writer = tagged("Activation", {"run_id": run,
                "activation_id": token(writer["payload"]["start_seq"])})
        elif writer["$tag"] == "Node":
            writer = tagged("Node", {"run_id": run, **writer["payload"]})
        emit("Knowledge", {"version": {"ref": ref,
            "value": tagged(old["key"], version["value"]), "written_by": writer}}, at)
        return ref

    for old in original:
        payload, at = old["payload"], old["arrival"]["journal_time"]
        if old["$tag"] == "Knowledge":
            commit(payload["version"], at)
        else:
            a, step = payload["activation"], payload["step"]
            cause = a["caused_by"]
            activation = {"run_id": run, "activation_id": token(a["start_seq"]),
                "role": a["role"], "producer": a["producer"], "loop": a["loop"],
                "caused_by": tagged("None") if cause is None else tagged("Some", {
                    "activation_id": token(cause["activation"]["start_seq"]),
                    "ordinal": cause["sequence_no"]})}
            kind, detail = step["$tag"], step["payload"]
            knowledge = tagged("Nothing")
            if kind == "Write":
                ref = commit(detail["value"], at)
                knowledge = tagged("One", ref)
                step = tagged("Write", {"key": detail["key"]})
            elif kind == "Read":
                knowledge = tagged("One", reference(detail["value"]))
                step = tagged("Read", {"key": detail["key"]})
            elif kind == "Start":
                trigger = "LidarData" if detail["topic"] == "scan" else next(
                    name for name in EVENTS if snake(name) == detail["topic"])
                step = tagged("Start", {"trigger": tagged(trigger)})
            elif kind == "Event":
                event_name = next(name for name in EVENTS if snake(name) == detail["topic"])
                step = tagged("Event", {"name": detail["topic"],
                    "payload": {"detail": tagged(event_name), **detail["message"]},
                    "emission": {"activation_id": activation["activation_id"],
                                 "ordinal": detail["sequence_no"]}})
            elif kind == "ServiceCompleted":
                step = tagged(kind, {**detail, "outcome": tagged(detail["outcome"])})
            else:
                step = resolved_refs(step)
            emit("Phase", {"activation": activation, "step": step, "knowledge": knowledge}, at)
        mapping.append(len(rows) - 1)
    return rows, mapping


EVENTS = ("ProcessedData", "AnomalyFound", "PlanningCompleted", "PlanAccepted", "PlanRejected")


if __name__ == "__main__":
    for scenario in ("accepted", "replanned", "unsafe", "stale", "slow", "unvalidated",
                     "missing-read", "retry-exhausted", "no-plan", "wrong-effect", "overlap",
                     "withdrawn", "pending-service", "not-delivered", "owner-ended", "service-conflict",
                     "external-plan", "false-anomaly", "same-value-update", "ineffective", "nominal"):
        rows, mapping = native_trace(scenario)
        native = ROOT / "native"
        native.mkdir(exist_ok=True)
        capture = native / f"{scenario}.jsonl"
        capture.write_text("".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows))
        manifest = {"records": len(rows), "sha256": hashlib.sha256(capture.read_bytes()).hexdigest(),
                    "journal_clock": CLOCK}
        (native / f"{scenario}-manifest.json").write_text(json.dumps({
            **manifest, "semantic_record_ticks": mapping,
            "archived_sha256": hashlib.sha256((ROOT / f"{scenario}.jsonl").read_bytes()).hexdigest(), "run_id": "tutorial-run",
            "store_epoch": EPOCH}, indent=2) + "\n")
        source = {"kind": "rpio-observation-clock-file", "variable": "native_observation", "journal_clock": CLOCK,
                  "files": [f"examples/turtlebot-maplek/native/{scenario}.jsonl"],
                  "capture_checks": [manifest]}
        (ROOT / f"{scenario}-input.json5").write_text(json.dumps({"sources": {"journal": source}}, indent=2) + "\n")
        if scenario in ("accepted", "overlap", "withdrawn", "pending-service", "not-delivered", "owner-ended", "service-conflict"):
            source = dict(source)
            if scenario == "pending-service":
                source["clock_samples"] = [{"time": t, "through": len(rows)} for t in (4.0, 5.0)]
            (ROOT / f"{scenario}-lifecycle-input.json5").write_text(json.dumps({"sources": {"journal": source}}, indent=2) + "\n")
