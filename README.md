
# SQL DATA WAREHOUSE AND ANALYTICS PROJECT
Building a modern data warehouse with MySQL including ETL, data modelling and analytics
This project demonstrates how a data warehouse is created from the start to the end using SQL Server and how the analytical insights can be taken out from the datasets.
The project requirements and the objective of this project are listed below.



## Project Requirements


### 1. Building the Data Warehouse (Data Engineering)

- **Goal**:  
        Build a modern data warehouse using **SQL Server** to combine sales data, create useful reports, and help make smart business decisions.

- **Details**:
  - **Data Sources**:  
        Use CSV files from two systems:  
          - ERP (for business operations)  
          - CRM (for customer data)

  - **Data Cleaning**:  
        Fix errors and make sure the data is clean before using it.

  - **Data Integration**:  
        Merge both sources into one simple and easy-to-use format for analysis.

  - **Scope**:  
        Only use the latest data. No need to include old or past records.

  - **Documentation**:  
        Write clear notes explaining how the data is organized, so business users and analysts can understand it easily.


### **2. BI: Analytics & Reporting (Data Analytics)**

- **Goal**:  
      Use **SQL queries** to find useful insights from the data.

- **Focus Areas**:
    - Customer Behavior  
    - Product Performance  
    - Sales Trends  

These insights will help teams understand key business metrics and make better decisions.

## Data Architecture
        This project follows medallion data architecture model which consists of Bronze, Silver and Gold Layers.
<img width="1501" height="730" alt="data architecture 2" src="https://github.com/user-attachments/assets/18cef6e1-8a16-4e66-8fd6-40ebd84fd479" />

**Bronze Layer**  : Keeps the raw data exactly as it comes from the source systems. The data is loaded from CSV files into the SQL Server database.
**Silver Layer**  : Cleans, standardizes, and organizes the data to make it ready for analysis.
**Gold Layer**    : Stores final, business-ready data arranged in a star schema for easy reporting and analysis.
