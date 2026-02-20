"""
Teste de Volume Databricks — Simulação das Queries do Power BI
==============================================================
Simula EXATAMENTE o que o Power BI vai fazer ao carregar os dados:
  - Base clientes: DISTINCT por SourceNumber_Customer + JOIN dim_customers
  - Ficha técnica (2): Fact filtrada (3 meses, flags S/N) + JOINs + LIMIT 5M

Requisitos:
  pip install databricks-sql-connector tabulate

Uso:
  set DATABRICKS_TOKEN=dapi...
  python test_databricks_volume.py
"""

from databricks import sql
from tabulate import tabulate
import os, sys, time

# ── Configuração ──────────────────────────────────────────────
HOST = "adb-7941093640821140.0.azuredatabricks.net"
HTTP_PATH = "/sql/1.0/warehouses/ce56ec5f5d0a3e07"
TOKEN = os.getenv("DATABRICKS_TOKEN", "")

LIMITE_LINHAS = 5_000_000
DATA_CORTE = "2025-11-19"  # 3 meses atrás de 19/02/2026
# ──────────────────────────────────────────────────────────────

def get_connection():
    """Conecta ao Databricks usando token (via DATABRICKS_TOKEN env var)."""
    if not TOKEN:
        print("❌ ERRO: Defina a variável DATABRICKS_TOKEN.")
        print("   Use: databricks auth login --host https://adb-7941093640821140.0.azuredatabricks.net")
        print("   E depois: $token = (databricks auth token --host ... | ConvertFrom-Json).access_token")
        sys.exit(1)
    print(f"🔌 Conectando ao Databricks (token: {len(TOKEN)} chars)...")
    return sql.connect(
        server_hostname=HOST,
        http_path=HTTP_PATH,
        access_token=TOKEN
    )

def run_query(cursor, label, query, show_sample=False):
    print(f"\n{'='*65}")
    print(f"  {label}")
    print(f"{'='*65}")
    print(f"  SQL: {query.strip()[:120]}...")
    start = time.time()
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        elapsed = time.time() - start
        
        if rows and len(rows[0]) == 1:
            count = rows[0][0]
            status = "✅ OK" if count <= LIMITE_LINHAS else "❌ EXCEDE LIMITE"
            print(f"  Resultado: {count:,.0f} linhas  ({status})")
            print(f"  ⏱️  Tempo: {elapsed:.2f}s")
            return count, elapsed
        else:
            if show_sample and rows:
                cols = [desc[0] for desc in cursor.description]
                print(tabulate(rows[:5], headers=cols, tablefmt="pipe"))
            print(f"  ⏱️  Tempo: {elapsed:.2f}s")
            return rows, elapsed
    except Exception as e:
        elapsed = time.time() - start
        print(f"  ❌ ERRO ({elapsed:.2f}s): {e}")
        return None, elapsed

def main():
    print("🔌 Conectando ao Databricks...")
    conn = get_connection()
    cursor = conn.cursor()

    results = {}

    # ══════════════════════════════════════════════════════════════
    # TESTE 1: Volume bruto das tabelas
    # ══════════════════════════════════════════════════════════════
    print("\n" + "█"*65)
    print("  TESTE 1: VOLUMES BRUTOS DAS TABELAS")
    print("█"*65)

    tabelas = {
        "fact_transaction_maintenance": "SELECT COUNT(*) FROM hive_metastore.gold.fact_transaction_maintenance",
        "dim_maintenance":              "SELECT COUNT(*) FROM hive_metastore.gold.dim_maintenance",
        "dim_customers":                "SELECT COUNT(*) FROM hive_metastore.gold.dim_customers",
        "dim_maintenancevehicles":       "SELECT COUNT(*) FROM hive_metastore.gold.dim_maintenancevehicles",
    }
    for nome, q in tabelas.items():
        count, _ = run_query(cursor, f"COUNT bruto: {nome}", q)
        results[nome] = count

    # ══════════════════════════════════════════════════════════════
    # TESTE 2: Fact COM filtro de 3 meses (sem LIMIT)
    # Simula o que o Power BI DEVERIA traduzir para SQL
    # ══════════════════════════════════════════════════════════════
    print("\n" + "█"*65)
    print(f"  TESTE 2: FACT COM FILTRO DE DATA >= {DATA_CORTE} (SEM LIMIT)")
    print("█"*65)

    sql_fact_filtered = f"""
    SELECT COUNT(*) FROM hive_metastore.gold.fact_transaction_maintenance
    WHERE Fl_Fatura_Cliente IN ('S','N')
      AND Fl_Fatura_Estabelecimento IN ('S','N')
      AND TransactionDate >= '{DATA_CORTE}'
    """
    count_fact_3m, _ = run_query(cursor, f"COUNT Fact filtrada ({DATA_CORTE})", sql_fact_filtered)
    results["fact_3meses_filtered"] = count_fact_3m

    # ══════════════════════════════════════════════════════════════
    # TESTE 3: Fact COM filtro + LIMIT 5M
    # ══════════════════════════════════════════════════════════════
    print("\n" + "█"*65)
    print(f"  TESTE 3: FACT COM FILTRO + LIMIT {LIMITE_LINHAS:,}")
    print("█"*65)

    sql_fact_limited = f"""
    SELECT COUNT(*) FROM (
        SELECT SourceNumber, Fl_Fatura_Cliente, Fl_Fatura_Estabelecimento,
               ItemApprovedAmount, TransactionDate
        FROM hive_metastore.gold.fact_transaction_maintenance
        WHERE Fl_Fatura_Cliente IN ('S','N')
          AND Fl_Fatura_Estabelecimento IN ('S','N')
          AND TransactionDate >= '{DATA_CORTE}'
        LIMIT {LIMITE_LINHAS}
    )
    """
    count_fact_ltd, _ = run_query(cursor, f"COUNT Fact filtrada + LIMIT {LIMITE_LINHAS:,}", sql_fact_limited)
    results["fact_3meses_limited"] = count_fact_ltd

    # ══════════════════════════════════════════════════════════════
    # TESTE 4: Query COMPLETA da Ficha Técnica (JOINs + filtros + LIMIT)
    # Exatamente o que o Power BI deveria executar
    # ══════════════════════════════════════════════════════════════
    print("\n" + "█"*65)
    print("  TESTE 4: QUERY COMPLETA — FICHA TÉCNICA (2)")
    print("█"*65)

    sql_ficha_completa = f"""
    SELECT COUNT(*) FROM (
        SELECT
            v.MaintenanceVehicleModelId,
            ft.Fl_Fatura_Cliente,
            ft.Fl_Fatura_Estabelecimento,
            CAST(ft.ItemApprovedAmount AS DOUBLE),
            CAST(m.SourceNumber_Customer AS INT),
            m.VehiclePlate,
            v.VehicleFamilyName,
            v.SegmentFamily,
            ft.TransactionDate,
            m.NameCustomer
        FROM (
            SELECT SourceNumber, Fl_Fatura_Cliente, Fl_Fatura_Estabelecimento,
                   ItemApprovedAmount, TransactionDate
            FROM hive_metastore.gold.fact_transaction_maintenance
            WHERE Fl_Fatura_Cliente IN ('S','N')
              AND Fl_Fatura_Estabelecimento IN ('S','N')
              AND TransactionDate >= '{DATA_CORTE}'
            LIMIT {LIMITE_LINHAS}
        ) ft
        LEFT JOIN hive_metastore.gold.dim_maintenance m
            ON ft.SourceNumber = m.SourceNumber_Transaction
        LEFT JOIN hive_metastore.gold.dim_maintenancevehicles v
            ON m.SourceNumber_Vehicle = v.VehicleSourceCode
    )
    """
    count_ft_full, t_ft = run_query(cursor, "COUNT Ficha técnica (query completa)", sql_ficha_completa)
    results["ficha_tecnica_full"] = count_ft_full

    # Amostra
    sql_ft_sample = f"""
    SELECT
        CAST(v.MaintenanceVehicleModelId AS INT) AS `Cód Veículo`,
        ft.Fl_Fatura_Cliente AS `FT?`,
        ft.Fl_Fatura_Estabelecimento AS `FT Novo?`,
        CAST(ft.ItemApprovedAmount AS DOUBLE) AS `Valor`,
        CAST(m.SourceNumber_Customer AS INT) AS `Cód Cliente`,
        m.VehiclePlate AS `Placa`,
        ft.TransactionDate AS `Data`
    FROM (
        SELECT SourceNumber, Fl_Fatura_Cliente, Fl_Fatura_Estabelecimento,
               ItemApprovedAmount, TransactionDate
        FROM hive_metastore.gold.fact_transaction_maintenance
        WHERE Fl_Fatura_Cliente IN ('S','N')
          AND Fl_Fatura_Estabelecimento IN ('S','N')
          AND TransactionDate >= '{DATA_CORTE}'
        LIMIT 5
    ) ft
    LEFT JOIN hive_metastore.gold.dim_maintenance m
        ON ft.SourceNumber = m.SourceNumber_Transaction
    LEFT JOIN hive_metastore.gold.dim_maintenancevehicles v
        ON m.SourceNumber_Vehicle = v.VehicleSourceCode
    """
    run_query(cursor, "AMOSTRA Ficha técnica (5 linhas)", sql_ft_sample, show_sample=True)

    # ══════════════════════════════════════════════════════════════
    # TESTE 5: Query COMPLETA da Base Clientes (DISTINCT + JOIN)
    # ══════════════════════════════════════════════════════════════
    print("\n" + "█"*65)
    print("  TESTE 5: QUERY COMPLETA — BASE CLIENTES")
    print("█"*65)

    sql_bc_completa = """
    SELECT COUNT(*) FROM (
        SELECT DISTINCT
            CAST(m.SourceNumber_Customer AS INT) AS CodCliente
        FROM hive_metastore.gold.dim_maintenance m
        WHERE m.SourceNumber_Customer IS NOT NULL
    )
    """
    count_bc, t_bc = run_query(cursor, "COUNT Base clientes (clientes únicos)", sql_bc_completa)
    results["base_clientes"] = count_bc

    # ══════════════════════════════════════════════════════════════
    # RESUMO FINAL
    # ══════════════════════════════════════════════════════════════
    print("\n" + "█"*65)
    print("  📊 RESUMO FINAL")
    print("█"*65)

    def fmt(v):
        return f"{v:,.0f}" if isinstance(v, (int, float)) else "?"

    def status(v):
        if not isinstance(v, (int, float)):
            return "?"
        return "✅ OK" if v <= LIMITE_LINHAS else "❌ EXCEDE"

    resumo = [
        ["fact (bruta, TOTAL)", fmt(results.get("fact_transaction_maintenance")), "-"],
        [f"fact (filtrada >= {DATA_CORTE})", fmt(results.get("fact_3meses_filtered")), status(results.get("fact_3meses_filtered"))],
        [f"fact (filtrada + LIMIT {LIMITE_LINHAS:,})", fmt(results.get("fact_3meses_limited")), status(results.get("fact_3meses_limited"))],
        ["Ficha técnica (query completa)", fmt(results.get("ficha_tecnica_full")), status(results.get("ficha_tecnica_full"))],
        ["Base clientes (distintos)", fmt(results.get("base_clientes")), status(results.get("base_clientes"))],
    ]
    print(tabulate(resumo, headers=["Descrição", "Linhas", "Status"], tablefmt="pipe"))

    # Diagnóstico
    fact_3m = results.get("fact_3meses_filtered")
    if isinstance(fact_3m, (int, float)):
        if fact_3m > LIMITE_LINHAS:
            print(f"\n⚠️  ATENÇÃO: A fact filtrada por 3 meses tem {fact_3m:,.0f} linhas.")
            print(f"   O LIMIT de {LIMITE_LINHAS:,} vai cortar para exatamente 5 milhões.")
            print(f"   Isso significa que ~{fact_3m - LIMITE_LINHAS:,.0f} linhas dos últimos 3 meses NÃO serão carregadas.")
        else:
            print(f"\n✅ PERFEITO: A fact filtrada por 3 meses tem {fact_3m:,.0f} linhas — cabe inteira no limite de {LIMITE_LINHAS:,}!")

    cursor.close()
    conn.close()
    print("\n🔌 Conexão fechada.")

if __name__ == "__main__":
    main()
