from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional
import re
from fastapi.security import HTTPBasic, HTTPBasicCredentials

app = FastAPI(
    title="Mini Data Query Simulation Engine",
    description="A simplified simulation of Gen AI Analytics data query system",
    version="1.0.0"
)
security = HTTPBasic()

# Mock in-memory database
mock_db = {
    "sales": {"columns": ["date", "amount", "region", "product"]},
    "customers": {"columns": ["id", "name", "email", "signup_date"]}
}

# Mock data responses
mock_responses = {
    "sales": [
        {"date": "2025-03-01", "amount": 1000, "region": "North", "product": "Widget"},
        {"date": "2025-03-02", "amount": 1500, "region": "South", "product": "Gadget"}
    ],
    "customers": [
        {"id": 1, "name": "John Doe", "email": "john@example.com", "signup_date": "2025-01-01"},
        {"id": 2, "name": "Jane Smith", "email": "jane@example.com", "signup_date": "2025-02-01"}
    ]
}

# Pydantic models
class QueryRequest(BaseModel):
    query: str

class QueryResponse(BaseModel):
    original_query: str
    translated_sql: str
    results: list

class ExplainResponse(BaseModel):
    original_query: str
    breakdown: dict
    translated_sql: str

class ValidateResponse(BaseModel):
    query: str
    is_valid: bool
    message: str

# Authentication
def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = "admin"
    correct_password = "secret123"
    if credentials.username != correct_username or credentials.password != correct_password:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return True

# Simple query parser
def parse_query_to_sql(query: str) -> tuple[str, str]:
    query = query.lower()
    
    # Basic pattern matching for common query types
    if "sales" in query:
        table = "sales"
        if "by region" in query:
            return "SELECT region, SUM(amount) FROM sales GROUP BY region", table
        elif "total" in query:
            return "SELECT SUM(amount) FROM sales", table
        elif "by product" in query:
            return "SELECT product, SUM(amount) FROM sales GROUP BY product", table
    elif "customer" in query or "customers" in query:
        table = "customers"
        if "count" in query:
            return "SELECT COUNT(*) FROM customers", table
        elif "new" in query or "recent" in query:
            return "SELECT * FROM customers ORDER BY signup_date DESC", table
    
    return "SELECT * FROM sales", "sales"  # Default fallback

# Query endpoint
@app.post("/query", response_model=QueryResponse)
async def process_query(request: QueryRequest, auth: bool = Depends(verify_credentials)):
    try:
        sql_query, table = parse_query_to_sql(request.query)
        return QueryResponse(
            original_query=request.query,
            translated_sql=sql_query,
            results=mock_responses.get(table, [])
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Query processing failed: {str(e)}")

# Explain endpoint
@app.post("/explain", response_model=ExplainResponse)
async def explain_query(request: QueryRequest, auth: bool = Depends(verify_credentials)):
    try:
        sql_query, table = parse_query_to_sql(request.query)
        breakdown = {
            "detected_table": table,
            "query_type": "aggregation" if "SUM" in sql_query else "selection",
            "columns": mock_db[table]["columns"]
        }
        return ExplainResponse(
            original_query=request.query,
            breakdown=breakdown,
            translated_sql=sql_query
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Query explanation failed: {str(e)}")

# Validate endpoint
@app.post("/validate", response_model=ValidateResponse)
async def validate_query(request: QueryRequest, auth: bool = Depends(verify_credentials)):
    query = request.query.lower()
    has_valid_table = any(table in query for table in mock_db.keys())
    
    if not query.strip():
        return ValidateResponse(query=request.query, is_valid=False, message="Query cannot be empty")
    if not has_valid_table:
        return ValidateResponse(query=request.query, is_valid=False, message="No valid table reference found")
    
    return ValidateResponse(query=request.query, is_valid=True, message="Query appears valid")

# Health check
@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)