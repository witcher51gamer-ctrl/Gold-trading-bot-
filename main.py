import os
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel

# 1. إعداد الاتصال بقاعدة البيانات على Railway
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./accounting.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# 2. نموذج قاعدة البيانات (جدول المعاملات)
class TransactionDB(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False)
    amount = Column(Float, nullable=False)
    type = Column(String, nullable=False) # 'income' أو 'expense'

Base.metadata.create_all(bind=engine)

# 3. نماذج Pydantic للتحقق من البيانات القادمة من تطبيق الجوال
class TransactionCreate(BaseModel):
    title: str
    amount: float
    type: str

class TransactionResponse(TransactionCreate):
    id: int
    class Config:
        from_attributes = True

# 4. إعداد تطبيق FastAPI
app = FastAPI(title="Accounting App API")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# 5. نقاط الاتصال (API Endpoints) لتطبيق الجوال

@app.get("/")
def root():
    return {"status": "Accounting API is running successfully"}

# إضافة عملية جديدة من الجوال
@app.post("/api/transactions", response_model=TransactionResponse)
def create_transaction(transaction: TransactionCreate, db: Session = Depends(get_db)):
    db_tx = TransactionDB(title=transaction.title, amount=transaction.amount, type=transaction.type)
    db.add(db_tx)
    db.commit()
    db.refresh(db_tx)
    return db_tx

# جلب كافة العمليات للتطبيق
@app.get("/api/transactions", response_model=list[TransactionResponse])
def get_transactions(db: Session = Depends(get_db)):
    return db.query(TransactionDB).all()

# جلب الملخص المالي (الإيرادات، المصروفات، الصافي)
@app.get("/api/summary")
def get_summary(db: Session = Depends(get_db)):
    transactions = db.query(TransactionDB).all()
    income = sum(t.amount for t in transactions if t.type == "income")
    expense = sum(t.amount for t in transactions if t.type == "expense")
    net = income - expense
    return {
        "total_income": income,
        "total_expense": expense,
        "net_profit": net
    }
