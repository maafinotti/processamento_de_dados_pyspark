# Carregamento do banco de dados de pagamentos

from pyspark.sql.types import *

arqschema = "anoref STRING, anocomp STRING, uf STRING, cmunicipio STRING, municipio STRING, cpf STRING, nis STRING, nome STRING, valor FLOAT"

pagamentos = spark.read.load("pagamentos_meses_bf.csv",format="csv", sep=";", schema=arqschema, header=False)

#Conerter para parket

pagamentos.write.parquet("pagamentos_meses_bf.parquet")

#Ler parquet

pagamentos = spark.read.parquet("pagamentos_meses_bf.parquet")









