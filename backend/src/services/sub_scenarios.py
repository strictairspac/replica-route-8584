from sqlalchemy import select
from fastapi import HTTPException
from src.models.entities import SubScenario
from src.utils.serialization import record
from .common import save


def listing(db):
    return [record(x) for x in
            db.scalars(select(SubScenario).order_by(SubScenario.id))]


def create(db, data, user):
    return save(db, SubScenario(**data.model_dump()), user,
                '创建订阅场景 ' + data.code)


def update(db, ident, data, user):
    obj = db.get(SubScenario, ident)
    if not obj:
        raise HTTPException(404, '订阅场景不存在')
    for k, v in data.model_dump().items():
        setattr(obj, k, v)
    return save(db, obj, user, '更新订阅场景 ' + data.code)
