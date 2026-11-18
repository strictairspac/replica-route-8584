from sqlalchemy import select
from redis import Redis
from fastapi import HTTPException
from src.models.entities import Node, SubScenario, SubTrial
from src.config.settings import REDIS_URL
from src.constants.enums import RUN_STATES
from src.utils.serialization import record
from .common import save
from .subscription import simulate

queue = Redis.from_url(REDIS_URL, decode_responses=True)
QUEUE_KEY = 'subdelivery:queue'


def listing(db):
    return [record(x) for x in
            db.scalars(select(SubTrial).order_by(SubTrial.id.desc()))]


def detail(db, ident):
    obj = db.get(SubTrial, ident)
    if not obj:
        raise HTTPException(404, '订阅演练不存在')
    return record(obj)


def create(db, data, user):
    scenario = db.get(SubScenario, data.scenario_id)
    if not scenario:
        raise HTTPException(404, '订阅场景不存在')
    nodes = [record(x) for x in db.scalars(select(Node).order_by(Node.id))]
    events = scenario.events
    try:
        simulate(nodes, events, policy=data.ack_policy,
                 delivery_ms=data.delivery_ms,
                 ack_timeout_ms=data.ack_timeout_ms,
                 reconcile_rtt_ms=data.reconcile_rtt_ms,
                 inflight_window=data.inflight_window)
    except ValueError as e:
        raise HTTPException(422, str(e))
    snapshot = {'nodes': nodes, 'scenario': record(scenario)}
    obj = SubTrial(**data.model_dump(), input_snapshot=snapshot,
                   status=RUN_STATES[0])
    result = save(db, obj, user, '订阅演练入队 ' + data.name)
    try:
        queue.rpush(QUEUE_KEY, str(obj.id))
    except Exception:
        obj.status = 'failed'
        obj.error = '队列暂时不可用，请重新创建'
        db.commit()
        raise HTTPException(503, obj.error)
    return result
