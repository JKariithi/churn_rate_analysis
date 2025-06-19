# Churn Rate Analysis
This project analyzes customer churn in a telecom dataset. It includes:
 1. Exploratory Data Analysis (EDA)
 2. Correlation insights
 3. A simple Machine Learning model
 4. A FastAPI backend connected to a MotherDuck warehouse for querying the data via API endpoints
    
---

## About the Data
The dataset contains customer information from a telecom company, including demographics, service usage, contract details, and billing.

### Overview of the Columns in the Dataset
| Column             | Description                                                                 |
| ------------------ | --------------------------------------------------------------------------- |
| `customerID`       | Unique identifier for each customer.                                        |
| `gender`           | Customer’s gender (Male or Female).                                     |
| `SeniorCitizen`    | Indicates if the customer is a senior (1 = Yes, 0 = No).                    |
| `Partner`          | Whether the customer has a spouse or partner.                               |
| `Dependents`       | Whether the customer has dependents (like children or others they support). |
| `tenure`           | Number of months the customer has stayed with the company.                  |
| `PhoneService`     | Whether the customer has a telephone service.                               |
| `MultipleLines`    | Whether the customer has multiple phone lines.                              |
| `InternetService`  | Type of internet service (DSL, Fiber optic, or No).                   |
| `OnlineSecurity`   | Whether the customer has online security (or no internet).                  |
| `OnlineBackup`     | Whether the customer has online backup service.                             |
| `DeviceProtection` | Whether the customer has device protection service.                         |
| `TechSupport`      | Whether the customer has tech support access.                               |
| `StreamingTV`      | Whether the customer streams TV.                                            |
| `StreamingMovies`  | Whether the customer streams movies.                                        |
| `Contract`         | Type of contract (Month-to-month, One year, Two year).                |
| `PaperlessBilling` | Whether billing is paperless.                                               |
| `PaymentMethod`    | Method of payment                 |
| `MonthlyCharges`   | Amount charged to the customer per month.                                   |
| `TotalCharges`     | Total amount charged to the customer to date.                               |
| `Churn`            | Whether the customer has left the company (Yes or No).                  |

---

## Project Structue
- customer_churn.csv -- Dataset used for analysis
- churn_rate_analysis.ipynb -- FastAPI app including SQL queries for interacting with MotherDuck
- README.md -- Project documentation
- .gitignore -- Specifies files excluded from Git tracking (.env file)

---

## Requirements

- Python 3.8+
- FastAPI
- Uvicorn
- DuckDB
- MotherDuck

---

## How to Run 
### 1. Launch FastAPI Server
 ```bash
uvicorn api:app --reload 
```
### 2. View the Documentation
Swagger UI: http://127.0.0.1:8000/docs

---
