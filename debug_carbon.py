"""Investigação: encontrar as colunas CORRETAS de Ficha Técnica no Databricks"""
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
    out = []

    # 1. Buscar colunas com "ficha" ou "ft" em dim_maintenancevehicles
    out.append("=== 1. dim_maintenancevehicles: Amostra para customer 60905 (primeiras colunas relevantes) ===")
    cursor.execute("""
        SELECT MaintenanceVehicleModelId, VehicleFamilyName, MaintenanceModelFamily, 
               MaintenanceVehicleModel, MaintenanceVehicleVersion, MaintenanceVehicleEngineSpec,
               VehicleSourceCode, CustomerId
        FROM hive_metastore.gold.dim_maintenancevehicles 
        WHERE CustomerId = 60905
        LIMIT 5
    """)
    cols = [d[0] for d in cursor.description]
    out.append(f"  Colunas: {cols}")
    rows = cursor.fetchall()
    for r in rows:
        out.append(f"  {list(r)}")

    # 2. Verificar quantos veículos com e sem MaintenanceVehicleModelId
    out.append("\n=== 2. Veículos do customer 60905: com vs sem MaintenanceVehicleModelId ===")
    cursor.execute("""
        SELECT 
            CASE WHEN MaintenanceVehicleModelId IS NOT NULL AND MaintenanceVehicleModelId > 0 THEN 'Tem ModelId' ELSE 'Sem ModelId' END as status,
            COUNT(*) as qtd
        FROM hive_metastore.gold.dim_maintenancevehicles 
        WHERE CustomerId = 60905
        GROUP BY CASE WHEN MaintenanceVehicleModelId IS NOT NULL AND MaintenanceVehicleModelId > 0 THEN 'Tem ModelId' ELSE 'Sem ModelId' END
    """)
    for r in cursor.fetchall(): out.append(f"  {r[0]}: {r[1]}")

    # 3. Na FACT, verificar a distribuição de Fl_Fatura_Cliente para Jan/2026 (customer 60905)
    out.append("\n=== 3. FACT JAN/2026: Distribuição flags para 60905 ===")
    cursor.execute("""
        SELECT ft.Fl_Fatura_Cliente, ft.Fl_Fatura_Estabelecimento, COUNT(*) as qtd, 
               SUM(CAST(ft.ItemApprovedAmount AS DOUBLE)) as va
        FROM hive_metastore.gold.fact_transaction_maintenance ft 
        LEFT JOIN hive_metastore.gold.dim_maintenance m ON ft.SourceNumber = m.SourceNumber_Transaction 
        WHERE m.SourceNumber_Customer = 60905
          AND ft.TransactionDate >= '2026-01-01' AND ft.TransactionDate < '2026-02-01'
        GROUP BY ft.Fl_Fatura_Cliente, ft.Fl_Fatura_Estabelecimento
        ORDER BY qtd DESC
    """)
    for r in cursor.fetchall(): out.append(f"  Fl_Cli={r[0]} | Fl_Est={r[1]} | Qtd={r[2]} | VA=R$ {r[3]:,.2f}")

    # 4. Buscar TODAS as colunas da fact_transaction_maintenance para ver se existe FT flag
    out.append("\n=== 4. TODAS colunas fact_transaction_maintenance ===")
    cursor.execute("SELECT * FROM hive_metastore.gold.fact_transaction_maintenance LIMIT 1")
    cols = [d[0] for d in cursor.description]
    for c in cols: out.append(f"  - {c}")

    # 5. MaintenanceVehicleModelId como proxy para FT
    out.append("\n=== 5. FACT JAN/2026 customer 60905: Transações COM vs SEM MaintenanceVehicleModelId ===")
    cursor.execute("""
        SELECT 
            CASE WHEN CAST(v.MaintenanceVehicleModelId AS INT) > 0 THEN 'Tem FT' ELSE 'Sem FT' END as status,
            COUNT(*) as qtd,
            SUM(CAST(ft.ItemApprovedAmount AS DOUBLE)) as va
        FROM hive_metastore.gold.fact_transaction_maintenance ft 
        LEFT JOIN hive_metastore.gold.dim_maintenance m ON ft.SourceNumber = m.SourceNumber_Transaction 
        LEFT JOIN hive_metastore.gold.dim_maintenancevehicles v ON m.SourceNumber_Vehicle = v.VehicleSourceCode 
        WHERE m.SourceNumber_Customer = 60905
          AND ft.TransactionDate >= '2026-01-01' AND ft.TransactionDate < '2026-02-01'
        GROUP BY CASE WHEN CAST(v.MaintenanceVehicleModelId AS INT) > 0 THEN 'Tem FT' ELSE 'Sem FT' END
    """)
    for r in cursor.fetchall(): out.append(f"  {r[0]}: Qtd={r[1]} | VA=R$ {r[2]:,.2f}")

    with open("ft_investigation.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(out))
    print("\n".join(out))
