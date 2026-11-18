from fastapi import APIRouter, Depends
from src.config.database import db_session
from src.utils.security import current, writer
from src.schemas.subscription import SubScenarioInput
from src.services import sub_scenarios as service

router = APIRouter()


@router.get('/sub_scenarios')
def listing(db=Depends(db_session), user=Depends(current)):
    return service.listing(db)


@router.post('/sub_scenarios', status_code=201)
def create(data: SubScenarioInput, db=Depends(db_session), user=Depends(writer)):
    return service.create(db, data, user)


@router.put('/sub_scenarios/{ident}')
def update(ident: int, data: SubScenarioInput, db=Depends(db_session),
           user=Depends(writer)):
    return service.update(db, ident, data, user)
