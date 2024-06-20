# Como Executar a Aplicação

## Passo 1: Build e Start dos Contêineres
Execute o comando:
```bash
docker compose up --build -d
```

## Passo 2: Acessar o Contêiner do Spark Master
Execute o comando:
```bash
docker exec -it spark-master bash
```

## Passo 3: Navegar até o Diretório de Trabalho
Execute o comando:
```bash
cd /spark-workspace
```

## Passo 4: Executar o ETL
Execute o comando:
```bash
python3 __main__.py
```

## Passo 5: Executar o Dashboard Interativo
Execute o comando:
```bash
streamlit run dashboard/main.py
```
