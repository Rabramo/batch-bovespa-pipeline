import pandas as pd

# Caminho do arquivo original
df = pd.read_parquet('data/raw/data_ref=2025-07-30/b3_ibov.parquet')

# Remove a coluna 'data_ref'
df = df.drop(columns=['data_ref'])

# Salva um novo arquivo corrigido
df.to_parquet('data/clean/b3_ibov.parquet', index=False)