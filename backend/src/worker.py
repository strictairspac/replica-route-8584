import traceback, json
from pathlib import Path
from src.config.database import Session
from src.models.entities import Trial, SubTrial
from src.services.trials import queue
from src.services.routing import simulate as route_simulate
from src.services.sub_trials import QUEUE_KEY
from src.services.subscription import simulate as sub_simulate


def run_route(ident):
    with Session() as db:
        obj = db.get(Trial, ident)
        obj.status = 'running'
        db.commit()
        snapshot = obj.input_snapshot
        delay, sticky = obj.max_delay_ms, obj.sticky_ms
    result = route_simulate(snapshot['nodes'],
                            snapshot['request_set']['events'], delay, sticky)
    with Session() as db:
        obj = db.get(Trial, ident)
        obj.result, obj.status = result, 'completed'
        db.commit()
    Path(f'/artifacts/route-{ident}.json').write_text(
        json.dumps(result, ensure_ascii=False, indent=2))


def run_subscription(ident):
    with Session() as db:
        obj = db.get(SubTrial, ident)
        obj.status = 'running'
        db.commit()
        snapshot = obj.input_snapshot
        kwargs = dict(policy=obj.ack_policy, delivery_ms=obj.delivery_ms,
                      ack_timeout_ms=obj.ack_timeout_ms,
                      reconcile_rtt_ms=obj.reconcile_rtt_ms,
                      inflight_window=obj.inflight_window)
    result = sub_simulate(snapshot['nodes'], snapshot['scenario']['events'],
                          **kwargs)
    with Session() as db:
        obj = db.get(SubTrial, ident)
        obj.result, obj.status = result, 'completed'
        db.commit()
    Path(f'/artifacts/sub-{ident}.json').write_text(
        json.dumps(result, ensure_ascii=False, indent=2))


JOBS = [
    ('replicaroute:queue', Trial, run_route),
    (QUEUE_KEY, SubTrial, run_subscription),
]

while True:
    Path('/tmp/worker-heartbeat').touch()
    for key, model, runner in JOBS:
        item = queue.blpop(key, timeout=1)
        if not item:
            continue
        ident = int(item[1])
        try:
            runner(ident)
        except Exception as e:
            traceback.print_exc()
            with Session() as db:
                obj = db.get(model, ident)
                if obj:
                    obj.status = 'failed'
                    obj.error = str(e)
                    db.commit()
