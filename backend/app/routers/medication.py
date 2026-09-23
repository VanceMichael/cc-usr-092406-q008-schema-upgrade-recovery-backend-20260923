from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from ..database import get_db
from ..models import MedicationRecord, Batch
from ..schemas import MedicationRecordCreate, MedicationRecordUpdate, MedicationRecordResponse

router = APIRouter(
    prefix="/api/medication-records",
    tags=["用药记录"]
)

@router.post("/", response_model=MedicationRecordResponse)
def create_medication_record(record: MedicationRecordCreate, db: Session = Depends(get_db)):
    db_batch = db.query(Batch).filter(Batch.id == record.batch_id).first()
    if not db_batch:
        raise HTTPException(status_code=404, detail="批次不存在")
    
    new_record = MedicationRecord(**record.dict())
    db.add(new_record)
    db.commit()
    db.refresh(new_record)
    return new_record

@router.get("/", response_model=List[MedicationRecordResponse])
def get_medication_records(skip: int = 0, limit: int = 100, batch_id: int = None, db: Session = Depends(get_db)):
    query = db.query(MedicationRecord)
    if batch_id:
        query = query.filter(MedicationRecord.batch_id == batch_id)
    records = query.offset(skip).limit(limit).all()
    return records

@router.get("/{record_id}/", response_model=MedicationRecordResponse)
def get_medication_record(record_id: int, db: Session = Depends(get_db)):
    record = db.query(MedicationRecord).filter(MedicationRecord.id == record_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="用药记录不存在")
    return record

@router.put("/{record_id}/", response_model=MedicationRecordResponse)
def update_medication_record(record_id: int, record: MedicationRecordUpdate, db: Session = Depends(get_db)):
    db_record = db.query(MedicationRecord).filter(MedicationRecord.id == record_id).first()
    if not db_record:
        raise HTTPException(status_code=404, detail="用药记录不存在")
    
    update_data = record.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_record, key, value)
    
    db.commit()
    db.refresh(db_record)
    return db_record

@router.delete("/{record_id}/")
def delete_medication_record(record_id: int, db: Session = Depends(get_db)):
    db_record = db.query(MedicationRecord).filter(MedicationRecord.id == record_id).first()
    if not db_record:
        raise HTTPException(status_code=404, detail="用药记录不存在")
    
    db.delete(db_record)
    db.commit()
    return {"message": "用药记录删除成功"}
