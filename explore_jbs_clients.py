"""Exploração simplificada: Nome Cliente JBS/CARBON"""
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

    print("=== FACT: Clientes com JBS ===")
    cursor.execute("SELECT DISTINCT NameCustomer FROM fcp_dev.pbi_fact_aderencia_ft_90d WHERE UPPER(NameCustomer) LIKE '%JBS%'")
    for r in cursor.fetchall():
        print(r[0])

    print("\n=== FACT: Clientes com CARBON ===")
    cursor.execute("SELECT DISTINCT NameCustomer FROM fcp_dev.pbi_fact_aderencia_ft_90d WHERE UPPER(NameCustomer) LIKE '%CARBON%'")
    rows = cursor.fetchall()
    print(rows if rows else "NENHUM")

    print("\n=== DIM: Clientes JBS ===")
    cursor.execute("""
        SELECT CAST(SourceNumber_Customer AS STRING), NameCustomer
        FROM fcp_dev.pbi_dim_clientes_unicos
        WHERE UPPER(NameCustomer) LIKE '%JBS%'
    """)
    for r in cursor.fetchall():
        print(f"{r[0]} | {r[1]}")

    print("\n=== DIM: Clientes CARBON ===")
    cursor.execute("""
        SELECT CAST(SourceNumber_Customer AS STRING), NameCustomer
        FROM fcp_dev.pbi_dim_clientes_unicos
        WHERE UPPER(NameCustomer) LIKE '%CARBON%'
    """)
    rows = cursor.fetchall()
    if rows:
        for r in rows:
            print(f"{r[0]} | {r[1]}")
    else:
        print("NENHUM")

    print("\n=== DIM: Grupos com JBS ===")
    cursor.execute("""
        SELECT CAST(SourceNumber_Customer AS STRING), NameCustomer, EconomicGroupName
        FROM fcp_dev.pbi_dim_clientes_unicos
        WHERE UPPER(EconomicGroupName) LIKE '%JBS%'
    """)
    rows = cursor.fetchall()
    if rows:
        for r in rows:
            print(f"{r[0]} | {r[1]} | Grupo: {r[2]}")
    else:
        print("NENHUM")
