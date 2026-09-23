from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from ..database import get_db
from ..models import HarvestSale, Batch
from ..schemas import HarvestSaleCreate, HarvestSaleUpdate, HarvestSaleResponse

router = APIRouter(
    prefix="/api/harvest-sales",
    tags=["出塘销售"]
)

@router.post("/", response_model=HarvestSaleResponse)
def create_harvest_sale(sale: HarvestSaleCreate, db: Session = Depends(get_db)):
    db_batch = db.query(Batch).filter(Batch.id == sale.batch_id).first()
    if not db_batch:
        raise HTTPException(status_code=404, detail="批次不存在")
    
    if sale.total_amount is None:
        sale.total_amount = sale.weight * sale.unit_price
    
    new_sale = HarvestSale(**sale.dict())
    db.add(new_sale)
    db.commit()
    db.refresh(new_sale)
    return new_sale

@router.get("/", response_model=List[HarvestSaleResponse])
def get_harvest_sales(skip: int = 0, limit: int = 100, batch_id: int = None, db: Session = Depends(get_db)):
    query = db.query(HarvestSale)
    if batch_id:
        query = query.filter(HarvestSale.batch_id == batch_id)
    sales = query.offset(skip).limit(limit).all()
    return sales

@router.get("/{sale_id}/", response_model=HarvestSaleResponse)
def get_harvest_sale(sale_id: int, db: Session = Depends(get_db)):
    sale = db.query(HarvestSale).filter(HarvestSale.id == sale_id).first()
    if not sale:
        raise HTTPException(status_code=404, detail="出塘销售记录不存在")
    return sale

@router.put("/{sale_id}/", response_model=HarvestSaleResponse)
def update_harvest_sale(sale_id: int, sale: HarvestSaleUpdate, db: Session = Depends(get_db)):
    db_sale = db.query(HarvestSale).filter(HarvestSale.id == sale_id).first()
    if not db_sale:
        raise HTTPException(status_code=404, detail="出塘销售记录不存在")
    
    update_data = sale.dict(exclude_unset=True)
    
    if 'weight' in update_data or 'unit_price' in update_data:
        weight = update_data.get('weight', db_sale.weight)
        unit_price = update_data.get('unit_price', db_sale.unit_price)
        update_data['total_amount'] = weight * unit_price
    
    for key, value in update_data.items():
        setattr(db_sale, key, value)
    
    db.commit()
    db.refresh(db_sale)
    return db_sale

@router.delete("/{sale_id}/")
def delete_harvest_sale(sale_id: int, db: Session = Depends(get_db)):
    db_sale = db.query(HarvestSale).filter(HarvestSale.id == sale_id).first()
    if not db_sale:
        raise HTTPException(status_code=404, detail="出塘销售记录不存在")
    
    db.delete(db_sale)
    db.commit()
    return {"message": "出塘销售记录删除成功"}
