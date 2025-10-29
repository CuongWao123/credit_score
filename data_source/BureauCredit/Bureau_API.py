from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware 
from pydantic import BaseModel
from typing import List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import uvicorn
import os

load_dotenv()

app = FastAPI(
    title="Bureau Credit API",
    description="API for querying bureau credit information",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"],  
)

# Database configuration - read from environment variables
DB_CONFIG = {
    "host": "data_source_postgres",
    "port":  "5432",
    "database": "BureauCredit",
    "user": "postgres",
    "password": "postgres"
}

# Pydantic models
class BureauBalance(BaseModel):
    months_balance: Optional[int]
    stat_for_bureau: Optional[str]

class BureauCredit(BaseModel):
    reco_id_curr: int
    reco_bureau_id: int
    credit_status: Optional[str]
    days_credit: Optional[int]
    credit_day_overdue: Optional[int]
    days_credit_enddate: Optional[int]
    days_enddate_fact: Optional[int]
    credit_limit_max_overdue: Optional[float]
    credit_prolong_count: Optional[int]
    credit_sum: Optional[float]
    credit_sum_debt: Optional[float]
    credit_sum_limit: Optional[float]
    credit_sum_overdue: Optional[float]
    credit_type: Optional[str]
    days_credit_update: Optional[int]
    annuity_payment: Optional[float]
    months_balance: Optional[int]
    stat_for_bureau: Optional[str]

class BureauResponse(BaseModel):
    success: bool
    count: int
    data: List[BureauCredit]

# Database connection helper
def get_db_connection():
    """Create database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Bureau Credit API",
        "version": "1.0.0",
        "endpoints": {
            "/bureau/{reco_id_curr}": "Get bureau credits by current loan ID",
            "/bureau/search": "Search bureau credits with filters",
            "/health": "Health check"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        conn = get_db_connection()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@app.get("/bureau/{reco_id_curr}", response_model=BureauResponse)
async def get_bureau_by_current_loan(reco_id_curr: int):
    """
    Get all bureau credit records for a specific current loan ID
    
    Args:
        reco_id_curr: Current loan ID
        
    Returns:
        List of bureau credit records with balance information
    """
    query = """
        SELECT 
            b.reco_id_curr,
            b.reco_bureau_id,
            b.credit_status,
            b.credit_currency,
            b.days_credit,
            b.credit_day_overdue,
            b.days_credit_enddate,
            b.days_enddate_fact,
            b.credit_limit_max_overdue, 
            b.credit_prolong_count,
            b.credit_sum,
            b.credit_sum_debt,
            b.credit_sum_limit,
            b.credit_sum_overdue,
            b.credit_type,
            b.days_credit_update,
            b.annuity_payment,
            bb.months_balance,
            bb.stat_for_bureau
        FROM bki AS b
        JOIN bki_balance AS bb 
            ON b.reco_bureau_id = bb.reco_bureau_id
        WHERE b.reco_id_curr = %s
        ORDER BY b.reco_bureau_id, bb.months_balance DESC
    """
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute(query, (reco_id_curr,))
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        if not results:
            raise HTTPException(
                status_code=404, 
                detail=f"No bureau records found for reco_id_curr: {reco_id_curr}"
            )
        
        return {
            "success": True,
            "count": len(results),
            "data": results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")

@app.get("/bureau/search", response_model=BureauResponse)
async def search_bureau_credits(
    reco_id_curr: Optional[int] = Query(None, description="Current loan ID"),
    credit_status: Optional[str] = Query(None, description="Credit status"),
    credit_type: Optional[str] = Query(None, description="Credit type"),
    min_credit_sum: Optional[float] = Query(None, description="Minimum credit sum"),
    max_credit_sum: Optional[float] = Query(None, description="Maximum credit sum"),
    has_overdue: Optional[bool] = Query(None, description="Has overdue (credit_day_overdue > 0)"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip")
):
    """
    Search bureau credits with multiple filters
    """
    query = """
        SELECT 
            b.reco_id_curr,
            b.reco_bureau_id,
            b.credit_status,
            b.days_credit,
            b.credit_day_overdue,
            b.days_credit_enddate,
            b.days_enddate_fact,
            b.credit_limit_max_overdue, 
            b.credit_prolong_count,
            b.credit_sum,
            b.credit_sum_debt,
            b.credit_sum_limit,
            b.credit_sum_overdue,
            b.credit_type,
            b.days_credit_update,
            b.annuity_payment,
            bb.months_balance,
            bb.stat_for_bureau
        FROM bki AS b
        JOIN bki_balance AS bb 
            ON b.reco_bureau_id = bb.reco_bureau_id
        WHERE 1=1
    """
    
    params = []
    
    if reco_id_curr is not None:
        query += " AND b.reco_id_curr = %s"
        params.append(reco_id_curr)
    
    if credit_status:
        query += " AND b.credit_status = %s"
        params.append(credit_status)
    
    if credit_type:
        query += " AND b.credit_type = %s"
        params.append(credit_type)
    
    if min_credit_sum is not None:
        query += " AND b.credit_sum >= %s"
        params.append(min_credit_sum)
    
    if max_credit_sum is not None:
        query += " AND b.credit_sum <= %s"
        params.append(max_credit_sum)
    
    if has_overdue is not None:
        if has_overdue:
            query += " AND b.credit_day_overdue > 0"
        else:
            query += " AND (b.credit_day_overdue = 0 OR b.credit_day_overdue IS NULL)"
    
    query += " ORDER BY b.reco_id_curr, b.reco_bureau_id, bb.months_balance DESC"
    query += " LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "count": len(results),
            "data": results
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")

@app.get("/bureau/{reco_id_curr}/summary")
async def get_bureau_summary(reco_id_curr: int):
    """
    Get summary statistics for bureau credits of a specific current loan
    """
    query = """
        SELECT 
            b.reco_id_curr,
            COUNT(DISTINCT b.reco_bureau_id) as total_credits,
            COUNT(DISTINCT CASE WHEN b.credit_status = 'Active' THEN b.reco_bureau_id END) as active_credits,
            SUM(b.credit_sum) as total_credit_sum,
            SUM(b.credit_sum_debt) as total_debt,
            SUM(b.credit_sum_overdue) as total_overdue,
            AVG(b.credit_day_overdue) as avg_days_overdue,
            MAX(b.credit_day_overdue) as max_days_overdue,
            COUNT(DISTINCT CASE WHEN b.credit_day_overdue > 0 THEN b.reco_bureau_id END) as credits_with_overdue
        FROM bki AS b
        WHERE b.reco_id_curr = %s
        GROUP BY b.reco_id_curr
    """
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute(query, (reco_id_curr,))
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not result:
            raise HTTPException(
                status_code=404, 
                detail=f"No bureau records found for reco_id_curr: {reco_id_curr}"
            )
        
        return {
            "success": True,
            "data": result
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=6666)