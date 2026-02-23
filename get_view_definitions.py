import os
from databricks import sql

def get_connection():
    return sql.connect(
        server_hostname="adb-7941093640821140.0.azuredatabricks.net",
        http_path="/sql/1.0/warehouses/ce56ec5f5d0a3e07",
        access_token=os.environ.get("DATABRICKS_TOKEN")
    )

with get_connection() as conn:
    cursor = conn.cursor()
    
    with open("clean_ddl.txt", "w", encoding="utf-8") as f:
        f.write("=== VIEW: pbi_dim_clientes_unicos ===\n")
        cursor.execute("SHOW CREATE TABLE fcp_dev.pbi_dim_clientes_unicos")
        row = cursor.fetchone()
        if row: f.write(row[0] + "\n\n")
        
        f.write("=== VIEW: pbi_fact_aderencia_ft_90d ===\n")
        cursor.execute("SHOW CREATE TABLE fcp_dev.pbi_fact_aderencia_ft_90d")
        row = cursor.fetchone()
        if row: f.write(row[0] + "\n\n")
