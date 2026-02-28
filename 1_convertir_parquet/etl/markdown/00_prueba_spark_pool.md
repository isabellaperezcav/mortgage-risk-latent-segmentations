```python
spark.conf.set(
    "fs.azure.account.auth.type.stsynapsemetadata.dfs.core.windows.net",
    "OAuth"
)
spark.conf.set(
    "fs.azure.account.oauth.provider.type.stsynapsemetadata.dfs.core.windows.net",
    "com.microsoft.azure.synapse.tokenlibrary.LinkedServiceBasedTokenProvider"
)
spark.conf.set(
    "spark.storage.synapse.linkedServiceName",
    "synapse-fanniemae-WorkspaceDefaultStorage"
)
```


    StatementMeta(sparkpool01, 0, 9, Finished, Available, Finished)



```python
df_test = spark.read.csv(
    "abfss://raw-csvs@stsynapsemetadata.dfs.core.windows.net/2019Q1.csv",
    sep="|",
    header=False
)
print(f"Filas: {df_test.count()}, Columnas: {len(df_test.columns)}")
df_test.show(5, truncate=False)
```


    StatementMeta(sparkpool01, 1, 2, Finished, Available, Finished)


    Filas: 10988084, Columnas: 110
    +----+------------+------+---+-----------------------------------------+-----+----+-----+-----+---------+----+----+----+------+------+----+----+----+------+----+----+----+----+----+----+----+----+----+----+----+----+-----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
    |_c0 |_c1         |_c2   |_c3|_c4                                      |_c5  |_c6 |_c7  |_c8  |_c9      |_c10|_c11|_c12|_c13  |_c14  |_c15|_c16|_c17|_c18  |_c19|_c20|_c21|_c22|_c23|_c24|_c25|_c26|_c27|_c28|_c29|_c30|_c31 |_c32|_c33|_c34|_c35|_c36|_c37|_c38|_c39|_c40|_c41|_c42|_c43|_c44|_c45|_c46|_c47|_c48|_c49|_c50|_c51|_c52|_c53|_c54|_c55|_c56|_c57|_c58|_c59|_c60|_c61|_c62|_c63|_c64|_c65|_c66|_c67|_c68|_c69|_c70|_c71|_c72|_c73|_c74|_c75|_c76|_c77|_c78|_c79|_c80|_c81|_c82|_c83|_c84|_c85|_c86|_c87|_c88|_c89|_c90|_c91|_c92|_c93|_c94|_c95|_c96|_c97|_c98|_c99|_c100|_c101|_c102|_c103|_c104|_c105|_c106|_c107|_c108|_c109|
    +----+------------+------+---+-----------------------------------------+-----+----+-----+-----+---------+----+----+----+------+------+----+----+----+------+----+----+----+----+----+----+----+----+----+----+----+----+-----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
    |NULL|100000913397|012019|C  |Jpmorgan Chase Bank, National Association|Other|NULL|5.875|5.875|324000.00|NULL|0.00|360 |092018|112018|3   |357 |357 |102048|80  |80  |2   |49  |692 |665 |N   |C   |PU  |1   |P   |CA  |40140|925 |NULL|FRM |N   |N   |NULL|NULL|00  |NULL|N   |NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|N   |NULL|NULL|NULL|NULL|7   |NULL|N   |NULL|NULL|NULL|NULL|A   |N   |NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL |NULL |N    |NULL |NULL |NULL |NULL |NULL |7    |NULL |
    |NULL|100000913397|022019|C  |Jpmorgan Chase Bank, National Association|Other|NULL|5.875|5.875|324000.00|NULL|0.00|360 |092018|112018|4   |356 |356 |102048|80  |80  |2   |49  |692 |665 |N   |C   |PU  |1   |P   |CA  |40140|925 |NULL|FRM |N   |N   |NULL|NULL|00  |NULL|N   |NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|N   |NULL|NULL|NULL|NULL|7   |NULL|N   |NULL|NULL|NULL|NULL|A   |N   |NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL |NULL |N    |NULL |NULL |NULL |NULL |NULL |7    |NULL |
    |NULL|100000913397|032019|C  |Jpmorgan Chase Bank, National Association|Other|NULL|5.875|5.875|324000.00|NULL|0.00|360 |092018|112018|5   |355 |355 |102048|80  |80  |2   |49  |692 |665 |N   |C   |PU  |1   |P   |CA  |40140|925 |NULL|FRM |N   |N   |NULL|NULL|00  |NULL|N   |NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|N   |NULL|NULL|NULL|NULL|7   |NULL|N   |NULL|NULL|NULL|NULL|A   |N   |NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL |NULL |N    |NULL |NULL |NULL |NULL |NULL |7    |NULL |
    |NULL|100000913397|042019|C  |Jpmorgan Chase Bank, National Association|Other|NULL|5.875|5.875|324000.00|NULL|0.00|360 |092018|112018|6   |354 |354 |102048|80  |80  |2   |49  |692 |665 |N   |C   |PU  |1   |P   |CA  |40140|925 |NULL|FRM |N   |N   |NULL|NULL|00  |NULL|N   |NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|N   |NULL|NULL|NULL|NULL|7   |NULL|N   |NULL|NULL|NULL|NULL|A   |N   |NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL |NULL |N    |NULL |NULL |NULL |NULL |NULL |7    |NULL |
    |NULL|100000913397|052019|C  |Jpmorgan Chase Bank, National Association|Other|NULL|5.875|5.875|324000.00|NULL|0.00|360 |092018|112018|7   |353 |353 |102048|80  |80  |2   |49  |692 |665 |N   |C   |PU  |1   |P   |CA  |40140|925 |NULL|FRM |N   |N   |NULL|NULL|00  |NULL|N   |NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|N   |NULL|NULL|NULL|NULL|7   |NULL|N   |NULL|NULL|NULL|NULL|A   |N   |NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL |NULL |N    |NULL |NULL |NULL |NULL |NULL |7    |NULL |
    +----+------------+------+---+-----------------------------------------+-----+----+-----+-----+---------+----+----+----+------+------+----+----+----+------+----+----+----+----+----+----+----+----+----+----+----+----+-----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
    only showing top 5 rows
    
    
