BQ_PROJECT = "data-prod-295413"

# Para adicionar uma nova tabela, basta incluir um novo dict nesta lista.
# Campos obrigatórios: bq_table, databricks_table
# Campo opcional:  mode (padrão: "overwrite")
#   - "overwrite": substitui a tabela inteira a cada execução
#                  (indicado para tabelas de referência e fatos)
#   - "append":    acrescenta registros sem deletar os existentes
TABLES = [
    {
        "bq_table": f"{BQ_PROJECT}.avenue_finance.fact_revenue_fpna",
        "databricks_table": "avenue_finance.fpna.fact_revenue_fpna",
        "mode": "overwrite",
    },
    {
        "bq_table": f"{BQ_PROJECT}.avenue_finance.dim_external_client",
        "databricks_table": "avenue_finance.fpna.dim_external_client",
        "mode": "overwrite",
    },
]
