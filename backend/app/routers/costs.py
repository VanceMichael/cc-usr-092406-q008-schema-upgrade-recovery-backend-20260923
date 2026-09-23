from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from ..database import get_db
from ..models import CostRecord, Batch
from ..schemas import CostRecordCreate, CostRecordUpdate, CostRecordResponse

router = APIRouter(
    prefix="/api/cost-records",
    tags=["成本核算"]
)

@router.post("/", response_model=CostRecordResponse)
def create_cost_record(record: CostRecordCreate, db: Session = Depends(get_db)):
    db_batch = db.query(Batch).filter(Batch.id == record.batch_id).first()
    if not db_batch:
        raise HTTPException(status_code=404, detail="批次不存在")
    
    new_record = CostRecord(**record.dict())
    db.add(new_record)
    db.commit()
    db.refresh(new_record)
    return new_record

@router.get("/", response_model=List[CostRecordResponse])
def get_cost_records(skip: int = 0, limit: int = 100, batch_id: int = None, cost_type: str = None, db: Session = Depends(get_db)):
    query = db.query(CostRecord)
    if batch_id:
        query = query.filter(CostRecord.batch_id == batch_id)
    if cost_type:
        query = query.filter(CostRecord.cost_type == cost_type)
    records = query.offset(skip).limit(limit).all()
    return records

@router.get("/{record_id}/", response_model=CostRecordResponse)
def get_cost_record(record_id: int, db: Session = Depends(get_db)):
    record = db.query(CostRecord).filter(CostRecord.id == record_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="成本记录不存在")
    return record

@router.put("/{record_id}/", response_model=CostRecordResponse)
def update_cost_record(record_id: int, record: CostRecordUpdate, db: Session = Depends(get_db)):
    db_record = db.query(CostRecord).filter(CostRecord.id == record_id).first()
    if not db_record:
        raise HTTPException(status_code=404, detail="成本记录不存在")
    
    update_data = record.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_record, key, value)
    
    db.commit()
    db.refresh(db_record)
    return db_record

@router.delete("/{record_id}/")
def delete_cost_record(record_id: int, db: Session = Depends(get_db)):
    db_record = db.query(CostRecord).filter(CostRecord.id == record_id).first()
    if not db_record:
        raise HTTPException(status_code=404, detail="成本记录不存在")
    
    db.delete(db_record)
    db.commit()
    return {"message": "成本记录删除成功"}
