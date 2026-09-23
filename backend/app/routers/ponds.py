from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from ..database import get_db
from ..models import Pond
from ..schemas import PondCreate, PondUpdate, PondResponse

router = APIRouter(
    prefix="/api/ponds",
    tags=["塘口管理"]
)

@router.post("/", response_model=PondResponse)
def create_pond(pond: PondCreate, db: Session = Depends(get_db)):
    db_pond = db.query(Pond).filter(Pond.name == pond.name).first()
    if db_pond:
        raise HTTPException(status_code=400, detail="塘口名称已存在")
    new_pond = Pond(**pond.dict())
    db.add(new_pond)
    db.commit()
    db.refresh(new_pond)
    return new_pond

@router.get("/", response_model=List[PondResponse])
def get_ponds(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    ponds = db.query(Pond).offset(skip).limit(limit).all()
    return ponds

@router.get("/{pond_id}/", response_model=PondResponse)
def get_pond(pond_id: int, db: Session = Depends(get_db)):
    pond = db.query(Pond).filter(Pond.id == pond_id).first()
    if not pond:
        raise HTTPException(status_code=404, detail="塘口不存在")
    return pond

@router.put("/{pond_id}/", response_model=PondResponse)
def update_pond(pond_id: int, pond: PondUpdate, db: Session = Depends(get_db)):
    db_pond = db.query(Pond).filter(Pond.id == pond_id).first()
    if not db_pond:
        raise HTTPException(status_code=404, detail="塘口不存在")
    
    update_data = pond.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_pond, key, value)
    
    db.commit()
    db.refresh(db_pond)
    return db_pond

@router.delete("/{pond_id}/")
def delete_pond(pond_id: int, db: Session = Depends(get_db)):
    db_pond = db.query(Pond).filter(Pond.id == pond_id).first()
    if not db_pond:
        raise HTTPException(status_code=404, detail="塘口不存在")
    
    db.delete(db_pond)
    db.commit()
    return {"message": "塘口删除成功"}
