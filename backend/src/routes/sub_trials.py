import json
from fastapi import APIRouter, Depends
from fastapi.responses import Response
from src.config.database import db_session
from src.utils.security import current, writer
from src.schemas.subscription import SubTrialInput
from src.services import sub_trials as service

router = APIRouter()


@router.get('/sub_trials')
def listing(db=Depends(db_session), user=Depends(current)):
    return service.listing(db)


@router.post('/sub_trials', status_code=201)
def create(data: SubTrialInput, db=Depends(db_session), user=Depends(writer)):
    return service.create(db, data, user)


@router.get('/sub_trials/{ident}')
def detail(ident: int, db=Depends(db_session), user=Depends(current)):
    return service.detail(db, ident)


@router.get('/sub_trials/{ident}/export')
def export(ident: int, db=Depends(db_session), user=Depends(current)):
    return Response(
        json.dumps(service.detail(db, ident), ensure_ascii=False, indent=2),
        media_type='application/json',
        headers={'Content-Disposition': f'attachment; filename="sub-{ident}.json"'})
