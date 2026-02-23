import os
from databricks import sql

def get_connection():
    return sql.connect(
        server_hostname="adb-7941093640821140.0.azuredatabricks.net",
        http_path="/sql/1.0/warehouses/ce56ec5f5d0a3e07",
        access_token=os.environ.get("DATABRICKS_TOKEN")
    )

sql_dim_clientes = """
CREATE OR REPLACE VIEW fcp_dev.pbi_dim_clientes_unicos AS 
SELECT
    CAST(m.SourceNumber_Customer AS INT) AS SourceNumber_Customer, 
    CASE 
        WHEN m.SourceNumber_Customer = 217743 THEN 'JBS NO CARBON'
        WHEN m.SourceNumber_Customer = 60905 THEN 'JBS'
        ELSE m.NameCustomer 
    END AS NameCustomer,
    c.EconomicGroupName, 
    CASE 
        WHEN m.SourceNumber_Customer = 217743 THEN 'JBS NON CARBON'
        WHEN m.SourceNumber_Customer = 60905 THEN 'JBS'
        ELSE c.CommercialGroupName 
    END AS CommercialGroupName
FROM (SELECT SourceNumber_Customer, MAX(NameCustomer) AS NameCustomer FROM hive_metastore.gold.dim_maintenance WHERE SourceNumber_Customer IS NOT NULL GROUP BY SourceNumber_Customer) m
LEFT JOIN (SELECT CustomerLegalName, MAX(EconomicGroupName) AS EconomicGroupName, MAX(CommercialGroupName) AS CommercialGroupName FROM hive_metastore.gold.dim_customers WHERE CustomerLegalName IS NOT NULL GROUP BY CustomerLegalName) c ON m.NameCustomer = c.CustomerLegalName
-- Filtrando apenas os dois clientes JBS do painel
WHERE m.SourceNumber_Customer IN (60905, 217743)
LIMIT 5000000
"""

sql_fact_aderencia = """
CREATE OR REPLACE VIEW fcp_dev.pbi_fact_aderencia_ft_90d AS 
SELECT 
    CAST(v.MaintenanceVehicleModelId AS INT) AS MaintenanceVehicleModelId, 
    ft.Fl_Fatura_Cliente, 
    ft.Fl_Fatura_Estabelecimento, 
    CAST(ft.ItemApprovedAmount AS DOUBLE) AS ItemApprovedAmount, 
    CAST(m.SourceNumber_Customer AS INT) AS SourceNumber_Customer, 
    m.VehiclePlate, 
    v.VehicleFamilyName, 
    v.SegmentFamily, 
    CAST(ft.TransactionDate AS TIMESTAMP) AS TransactionDate, 
    CASE 
        WHEN m.SourceNumber_Customer = 217743 THEN 'JBS NO CARBON'
        WHEN m.SourceNumber_Customer = 60905 THEN 'JBS'
        ELSE m.NameCustomer 
    END AS NameCustomer 
FROM hive_metastore.gold.fact_transaction_maintenance ft 
LEFT JOIN hive_metastore.gold.dim_maintenance m ON ft.SourceNumber = m.SourceNumber_Transaction 
LEFT JOIN hive_metastore.gold.dim_maintenancevehicles v ON m.SourceNumber_Vehicle = v.VehicleSourceCode 
WHERE ft.TransactionDate >= DATE_ADD(CURRENT_DATE(), -150)
  -- Filtrando apenas os dois clientes JBS do painel
  AND m.SourceNumber_Customer IN (60905, 217743)
LIMIT 15000000
"""

print("Connecting to Databricks...")
try:
    with get_connection() as conn:
        cursor = conn.cursor()
        
        print("Executing CREATE OR REPLACE VIEW: fcp_dev.pbi_dim_clientes_unicos...")
        cursor.execute(sql_dim_clientes)
        print("Success: pbi_dim_clientes_unicos updated.")
        
        print("Executing CREATE OR REPLACE VIEW: fcp_dev.pbi_fact_aderencia_ft_90d...")
        cursor.execute(sql_fact_aderencia)
        print("Success: pbi_fact_aderencia_ft_90d updated.")
        
except Exception as e:
    print(f"Error during execution: {str(e)}")
