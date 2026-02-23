import os
from databricks import sql

def get_connection():
    return sql.connect(
        server_hostname="adb-7941093640821140.0.azuredatabricks.net",
        http_path="/sql/1.0/warehouses/ce56ec5f5d0a3e07",
        access_token=os.environ.get("DATABRICKS_TOKEN")
    )

def test_view_volume():
    """Valida volume de dados nas VIEWs recém-atualizadas"""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Testando dimensão de clientes
        cursor.execute("SELECT COUNT(*) FROM fcp_dev.pbi_dim_clientes_unicos")
        dim_count = cursor.fetchone()[0]
        print(f"Total de registros Dim Clientes (pbi_dim_clientes_unicos): {dim_count}")
        assert dim_count > 0, "VIEW de dimensão está vazia!"
        
        # Testando a fato de transações
        cursor.execute("SELECT COUNT(*) FROM fcp_dev.pbi_fact_aderencia_ft_90d")
        fact_count = cursor.fetchone()[0]
        print(f"Total de registros Fact Aderencia (pbi_fact_aderencia_ft_90d): {fact_count}")
        assert fact_count > 0, "VIEW de fato está vazia!"
        assert fact_count < 5_000_000, f"VIEW excede o limite estrito do Power BI de 5 milhões: {fact_count}"
        
        print("\nTodos os testes de volume e limites passaram com sucesso!")

if __name__ == "__main__":
    test_view_volume()
