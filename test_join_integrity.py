"""
Script de diagnóstico: Verifica integridade dos JOINs entre tabelas Databricks.
Identifica onde os dados se perdem no pipeline de dados.

Requisitos:
  pip install databricks-sql-connector tabulate

Uso:
  python test_join_integrity.py
"""

from databricks import sql
from tabulate import tabulate
import os, sys

# ── Configuração ──────────────────────────────────────────────
HOST = "adb-7941093640821140.0.azuredatabricks.net"
HTTP_PATH = "/sql/1.0/warehouses/ce56ec5f5d0a3e07"
TOKEN = os.getenv("DATABRICKS_TOKEN", "")

# ──────────────────────────────────────────────────────────────

def get_connection():
    if not TOKEN:
        print("❌ ERRO: Defina a variável DATABRICKS_TOKEN ou edite o script com seu token.")
        sys.exit(1)
    return sql.connect(
        server_hostname=HOST,
        http_path=HTTP_PATH,
        access_token=TOKEN
    )

def run_query(cursor, label, query, show_sample=False):
    print(f"\n{'─'*60}")
    print(f"  {label}")
    print(f"{'─'*60}")
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        if show_sample and rows:
            cols = [desc[0] for desc in cursor.description]
            print(tabulate(rows, headers=cols, tablefmt="pipe"))
        elif rows and len(rows[0]) == 1:
            print(f"  → {rows[0][0]:,}")
            return rows[0][0]
        else:
            cols = [desc[0] for desc in cursor.description]
            print(tabulate(rows, headers=cols, tablefmt="pipe"))
        return rows
    except Exception as e:
        print(f"  ❌ ERRO: {e}")
        return None

def main():
    print("🔌 Conectando ao Databricks...")
    conn = get_connection()
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("  TESTE 1: TIPOS DAS COLUNAS DE JOIN")
    print("="*60)
    
    # Verificar tipos das colunas de JOIN
    run_query(cursor, "Tipo de SourceNumber em fact_transaction_maintenance",
        "DESCRIBE hive_metastore.gold.fact_transaction_maintenance", show_sample=True)
    
    run_query(cursor, "Tipo de SourceNumber_Transaction em dim_maintenance",
        "DESCRIBE hive_metastore.gold.dim_maintenance", show_sample=True)
    
    run_query(cursor, "Tipo de VehicleSourceCode em dim_maintenancevehicles",
        "DESCRIBE hive_metastore.gold.dim_maintenancevehicles", show_sample=True)
    
    print("\n" + "="*60)
    print("  TESTE 2: MATCH RATE DOS JOINs")
    print("="*60)
    
    # JOIN 1: fact_transaction → dim_maintenance
    run_query(cursor, "fact → dim_maintenance: linhas com match",
        """
        SELECT 
            COUNT(*) AS total_fact,
            SUM(CASE WHEN m.SourceNumber_Transaction IS NOT NULL THEN 1 ELSE 0 END) AS com_match,
            SUM(CASE WHEN m.SourceNumber_Transaction IS NULL THEN 1 ELSE 0 END) AS sem_match,
            ROUND(SUM(CASE WHEN m.SourceNumber_Transaction IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_match
        FROM hive_metastore.gold.fact_transaction_maintenance ft
        LEFT JOIN hive_metastore.gold.dim_maintenance m
            ON ft.SourceNumber = m.SourceNumber_Transaction
        WHERE ft.Fl_Fatura_Cliente IN ('S','N')
          AND ft.Fl_Fatura_Estabelecimento IN ('S','N')
          AND ft.TransactionDate >= DATEADD(DAY, -365, CURRENT_DATE())
        """, show_sample=True)
    
    # JOIN 2: dim_maintenance → dim_maintenancevehicles
    run_query(cursor, "dim_maintenance → dim_maintenancevehicles: linhas com match",
        """
        SELECT
            COUNT(*) AS total_maintenance,
            SUM(CASE WHEN v.VehicleSourceCode IS NOT NULL THEN 1 ELSE 0 END) AS com_match,
            SUM(CASE WHEN v.VehicleSourceCode IS NULL THEN 1 ELSE 0 END) AS sem_match,
            ROUND(SUM(CASE WHEN v.VehicleSourceCode IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_match
        FROM hive_metastore.gold.dim_maintenance m
        LEFT JOIN hive_metastore.gold.dim_maintenancevehicles v
            ON m.SourceNumber_Vehicle = v.VehicleSourceCode
        """, show_sample=True)
    
    # JOIN 3: Base clientes ↔ Grupo Econômico
    run_query(cursor, "dim_maintenance → dim_customers: match por nome",
        """
        SELECT
            COUNT(DISTINCT m.NameCustomer) AS total_nomes_maintenance,
            COUNT(DISTINCT c.CustomerLegalName) AS total_nomes_customers,
            SUM(CASE WHEN c.CustomerLegalName IS NOT NULL THEN 1 ELSE 0 END) AS com_match,
            SUM(CASE WHEN c.CustomerLegalName IS NULL THEN 1 ELSE 0 END) AS sem_match
        FROM (SELECT DISTINCT NameCustomer FROM hive_metastore.gold.dim_maintenance WHERE NameCustomer IS NOT NULL) m
        LEFT JOIN (SELECT DISTINCT CustomerLegalName FROM hive_metastore.gold.dim_customers WHERE CustomerLegalName IS NOT NULL) c
            ON m.NameCustomer = c.CustomerLegalName
        """, show_sample=True)
    
    print("\n" + "="*60)
    print("  TESTE 3: FILTRO JBS")
    print("="*60)
    
    # Verificar se JBS existe como Grupo Econômico
    run_query(cursor, "Buscar 'JBS' em dim_customers.CommercialGroupName",
        """
        SELECT DISTINCT CommercialGroupName, EconomicGroupName
        FROM hive_metastore.gold.dim_customers
        WHERE UPPER(CommercialGroupName) LIKE '%JBS%'
           OR UPPER(EconomicGroupName) LIKE '%JBS%'
        LIMIT 20
        """, show_sample=True)
    
    # Verificar se existem transações para clientes JBS
    run_query(cursor, "Transações de clientes JBS (via dim_maintenance → dim_customers)",
        """
        SELECT COUNT(*) AS total_transacoes_jbs
        FROM hive_metastore.gold.fact_transaction_maintenance ft
        JOIN hive_metastore.gold.dim_maintenance m
            ON ft.SourceNumber = m.SourceNumber_Transaction
        JOIN (
            SELECT DISTINCT CustomerLegalName, CommercialGroupName
            FROM hive_metastore.gold.dim_customers
            WHERE UPPER(CommercialGroupName) LIKE '%JBS%'
        ) c ON m.NameCustomer = c.CustomerLegalName
        WHERE ft.Fl_Fatura_Cliente IN ('S','N')
          AND ft.Fl_Fatura_Estabelecimento IN ('S','N')
          AND ft.TransactionDate >= DATEADD(DAY, -365, CURRENT_DATE())
        """)
    
    print("\n" + "="*60)
    print("  TESTE 4: VALIDAÇÃO FICHA TÉCNICA (possui_FT)")
    print("="*60)
    
    # Verificar valores de Fl_Fatura_Cliente e Fl_Fatura_Estabelecimento
    run_query(cursor, "Valores distintos de Fl_Fatura_Cliente",
        """
        SELECT Fl_Fatura_Cliente, COUNT(*) AS qtd
        FROM hive_metastore.gold.fact_transaction_maintenance
        GROUP BY Fl_Fatura_Cliente
        ORDER BY qtd DESC
        """, show_sample=True)
    
    run_query(cursor, "Valores distintos de Fl_Fatura_Estabelecimento",
        """
        SELECT Fl_Fatura_Estabelecimento, COUNT(*) AS qtd
        FROM hive_metastore.gold.fact_transaction_maintenance
        GROUP BY Fl_Fatura_Estabelecimento
        ORDER BY qtd DESC
        """, show_sample=True)
    
    # Verificar combinação S/N para lógica possui_FT
    run_query(cursor, "Combinação Fl_Fatura_Cliente x Fl_Fatura_Estabelecimento (para possui_FT)",
        """
        SELECT 
            ft.Fl_Fatura_Cliente,
            ft.Fl_Fatura_Estabelecimento,
            COUNT(*) AS qtd
        FROM hive_metastore.gold.fact_transaction_maintenance ft
        WHERE ft.Fl_Fatura_Cliente IN ('S','N')
          AND ft.Fl_Fatura_Estabelecimento IN ('S','N')
          AND ft.TransactionDate >= DATEADD(DAY, -365, CURRENT_DATE())
        GROUP BY ft.Fl_Fatura_Cliente, ft.Fl_Fatura_Estabelecimento
        ORDER BY qtd DESC
        """, show_sample=True)
    
    print("\n" + "="*60)
    print("  TESTE 5: RELATIONSHIP Código Cliente ↔ Cód. Cliente")
    print("="*60)
    
    # Verificar se SourceNumber_Customer (Código Cliente em FT) faz match com os clientes distintos
    run_query(cursor, "Match de SourceNumber_Customer entre FT e Base Clientes",
        """
        WITH ft_customers AS (
            SELECT DISTINCT m.SourceNumber_Customer
            FROM hive_metastore.gold.fact_transaction_maintenance ft
            JOIN hive_metastore.gold.dim_maintenance m
                ON ft.SourceNumber = m.SourceNumber_Transaction
            WHERE ft.Fl_Fatura_Cliente IN ('S','N')
              AND ft.Fl_Fatura_Estabelecimento IN ('S','N')
              AND ft.TransactionDate >= DATEADD(DAY, -365, CURRENT_DATE())
              AND m.SourceNumber_Customer IS NOT NULL
        ),
        base_clientes AS (
            SELECT DISTINCT SourceNumber_Customer
            FROM hive_metastore.gold.dim_maintenance
            WHERE SourceNumber_Customer IS NOT NULL
        )
        SELECT
            (SELECT COUNT(*) FROM ft_customers) AS clientes_em_ft,
            (SELECT COUNT(*) FROM base_clientes) AS clientes_em_base,
            (SELECT COUNT(*) FROM ft_customers f JOIN base_clientes b ON f.SourceNumber_Customer = b.SourceNumber_Customer) AS match_count
        """, show_sample=True)
    
    cursor.close()
    conn.close()
    print("\n🔌 Conexão fechada.")
    print("\n✅ Diagnóstico completo!")

if __name__ == "__main__":
    main()
