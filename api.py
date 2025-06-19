from fastapi import FastAPI, HTTPException
import duckdb
import os
from dotenv import load_dotenv

# Initializing  FastAPI app
app = FastAPI(title="Customer Churn API")

# loading the token to a seperate .env file
load_dotenv()
token = os.getenv('MOTHERDUCK_TOKEN')
if not token:
    raise ValueError("MOTHERDUCK_TOKEN not found in .env file")

# Connecting  to MotherDuck
def get_db_connection():
    try:
        con = duckdb.connect(f'md:churn_data?motherduck_token={token}')
        return con
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")
    
# Endpont 0: Creating customer_churn table 
@app.get("/create_churn_table")
async def create_churn_table():
    con = get_db_connection()
    try:
        con.execute("""
    CREATE TABLE IF NOT EXISTS customer_churn (
        customerID VARCHAR,
        gender VARCHAR,
        SeniorCitizen INTEGER,
        Partner VARCHAR,
        Dependents VARCHAR,
        tenure INTEGER,
        PhoneService VARCHAR,
        MultipleLines VARCHAR,
        InternetService VARCHAR,
        OnlineSecurity VARCHAR,
        OnlineBackup VARCHAR,
        DeviceProtection VARCHAR,
        TechSupport VARCHAR,
        StreamingTV VARCHAR,
        StreamingMovies VARCHAR,
        Contract VARCHAR,
        PaperlessBilling VARCHAR,
        PaymentMethod VARCHAR,
        MonthlyCharges DECIMAL(10,2),
        TotalCharges DECIMAL(10,2),
        Churn VARCHAR
    )
""")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    finally:
        con.close()

# Endpont 1: Importing data from customer_churn.csv
@app.get("/import_data")
async def import_data():
    con = get_db_connection()
    try:
        con.execute("""
    COPY customer_churn FROM 'customer_churn.csv' (AUTO_DETECT TRUE)
    """)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    finally:
        con.close()

# Endpoint 2: Churn Rate by  Contract type
@app.get("/churn_by_contract_type")
async def churn_by_contract_type():
    con = get_db_connection()
    try:
        results = con.execute("""
            SELECT 
                Contract,
                COUNT(*) AS total_customers,
                ROUND(AVG(MonthlyCharges), 2) as avg_monthly_charges,
                SUM(CASE WHEN Churn = 'Yes' THEN 1 ELSE 0 END) AS churned_customers,
                ROUND(100.0 * SUM(CASE WHEN Churn = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 2) AS churn_rate_percent
            FROM churn_data.main.customer_churn
            GROUP BY  Contract
            ORDER BY churn_rate_percent DESC;
        """).fetchall()
        # Convert to list of dicts for JSON response
        return [
            {
                "Contract": row[0],
                "total_customers": row[1],
                "avg_monthly_charges": row[2],
                "churned_customers": row[3],
                "churn_rate_percent": row[4]
            } for row in results
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    finally:
        con.close()

# Endpoint 3: Average Tenure by Payment Method
@app.get("/tenure_by_payment_method")
async def tenure_by_payment_method():
    con = get_db_connection()
    try:
        results = con.execute("""
            SELECT PaymentMethod,
                   ROUND(AVG(tenure), 2) AS avg_tenure_months,
                   COUNT(*) AS customer_count
            FROM churn_data.main.customer_churn
            GROUP BY PaymentMethod
            ORDER BY avg_tenure_months DESC
        """).fetchall()
        return [
            {
                "PaymentMethod": row[0],
                "avg_tenure_months": row[1],
                "customer_count": row[2]
            } for row in results
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    finally:
        con.close()

# Endpoint 4: filling missing values in Total Charges column 
@app.get("/filing_missing_values")
async def filling_missing_values():
    con = get_db_connection()
    try:
        results = con.execute("""
            UPDATE churn_data.main.customer_churn
            SET TotalCharges = 0
            WHERE TotalCharges IS NULL AND tenure = 0;  
        """).fetchall()
        return [
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    finally:
        con.close()

# Endpoint 5: Churn Rate by Service Combination
@app.get("/churn_by_service_combination")
async def churn_by_service_combination():
    con = get_db_connection()
    try:
        results = con.execute("""
            SELECT 
             PhoneService, MultipleLines, InternetService, OnlineSecurity, OnlineBackup, DeviceProtection, TechSupport, StreamingTV, StreamingMovies,
             ROUND(AVG(MonthlyCharges)) AS average_monthly_charges,
             COUNT(*) AS total_customers,
             SUM(CASE WHEN Churn = 'Yes' THEN 1 ELSE 0 END) AS churned_customers,
             ROUND(100.0 * SUM(CASE WHEN Churn = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 2) AS churn_rate_percent
            FROM churn_data.main.customer_churn
            GROUP BY ALL
            ORDER BY total_customers DESC ,churn_rate_percent
        """).fetchall()
        return [
            {
                "PhoneService": row[0],
                "MultipleLines": row[1],
                "InternetService": row[2],
                "OnlineSecurity": row[3],
                "OnlineBackup": row[4],
                "DeviceProtection": row[5],
                "TechSupport": row[6],
                "StreamingTV": row[7],
                "StreamingMovies": row[8],
                "average_monthly_charges": row[9],
                "total_customers": row[10],
                "churned_customers": row[11],
                "churn_rate_percent":row[12]
            } for row in results
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    finally:
        con.close()

# Endpoint 6: Churn Rate by Dependents and Partners
@app.get("/churn_by_dependents_partner")
async def churn_by_dependents_partner():
    con = get_db_connection()
    try:
        results = con.execute("""
            SELECT 
               Dependents,
               Partner, 
               COUNT(*) total_customers,
               SUM(CASE WHEN Churn = 'Yes' THEN 1 ELSE 0 END) AS churned_customers,
               ROUND(100.0 * SUM(CASE WHEN Churn = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 2) AS churn_rate_percent
            FROM churn_data.main.customer_churn
            GROUP BY ALL
            ORDER BY churn_rate_percent DESC;
        """).fetchall()
        return [
            {
              "Dependents": row[0],
              "Partner": row[1],
              "total_customers": row[2],
              "churned_customers":row[3],
              "churn_rate_percent": row[4]
            } for row in results
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    finally:
        con.close()

# Endpoint 7: Churn Rate by Age
@app.get("/churn_by_age")
async def churn_by_age():
    con = get_db_connection()
    try:
        results = con.execute("""
            SELECT
              SeniorCitizen,
              COUNT(*) AS total_customers,
              SUM(CASE WHEN Churn = 'Yes' THEN 1 ELSE 0 END) AS churned_customers,
              ROUND(100.0 * SUM(CASE WHEN Churn = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 2) AS churn_rate_percent
            FROM churn_data.main.customer_churn
            GROUP BY SeniorCitizen
        """).fetchall()
        return [
            {
              "SeniorCitizen": row[0],
              "total_customers": row[1],
              "churned_customers":row[2],
              "churn_rate_percent": row[3]
            } for row in results
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    finally:
        con.close()

# Endpoint 8: Churn Rate by Gender
@app.get("/churn_by_gender")
async def churn_by_gender():
    con = get_db_connection()
    try:
        results = con.execute("""
            SELECT
              gender,
              COUNT(*) AS total_customers,
              SUM(CASE WHEN Churn = 'Yes' THEN 1 ELSE 0 END) AS churned_customers,
              ROUND(100.0 * SUM(CASE WHEN Churn = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 2) AS churn_rate_percent
            FROM churn_data.main.customer_churn
            GROUP BY gender
        """).fetchall()
        return [
            {
              "gender": row[0],
              "total_customers": row[1],
              "churned_customers":row[2],
              "churn_rate_percent": row[3]
            } for row in results
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    finally:
        con.close()

# Endpoint 9: Churn Rate by Tenure
@app.get("/churn_by_tenure")
async def churn_by_tenure():
    con = get_db_connection()
    try:
        results = con.execute("""
            SELECT                    
               Churn,
               COUNT(*) AS total_customers,
               ROUND(AVG(tenure)) AS avg_tenure,
               ROUND(AVG(MonthlyCharges), 2) AS avg_monthly_charges,
               ROUND(AVG(TotalCharges), 2) AS avg_total_charges
            FROM churn_data.main.customer_churn 
            GROUP BY Churn
        """).fetchall()
        return [
            {
              "Churn": row[0],
              "total_customers": row[1],
              "avg_tenure":row[2],
              "avg_monthly_charges": row[3],
              "avg_total_churges": row[4]
            } for row in results
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    finally:
        con.close()




