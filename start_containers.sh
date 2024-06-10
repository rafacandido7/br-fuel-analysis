#!/bin/bash

echo "Iniciando Ambiente..."

# Tkinter instalation
sudo apt-get install python3-tk

docker-compose -p fuel-analysis up -d

echo "Montando containers..."

MAX_WAIT=60
SECONDS_WAITED=0
SLEEP_INTERVAL=2

CONTAINER_NAME=$(docker-compose ps -q jupyter)

while ! docker logs $CONTAINER_NAME 2>&1 | grep -q 'token='; do
  if [ $SECONDS_WAITED -ge $MAX_WAIT ]; then
    echo "Erro: o contêiner não ficou pronto a tempo."
    exit 1
  fi
  echo "Aguardando contêiner iniciar... ($SECONDS_WAITED/$MAX_WAIT segundos)"
  sleep $SLEEP_INTERVAL
  SECONDS_WAITED=$((SECONDS_WAITED + SLEEP_INTERVAL))
done

echo "Obtendo o token do JupyterLab..."
TOKEN=$(docker logs $CONTAINER_NAME 2>&1 | grep -o 'token=[a-zA-Z0-9]\+' | head -1 | cut -d'=' -f2)

if [ -z "$TOKEN" ]; then
  echo "Erro: não foi possível obter o token do JupyterLab."
  exit 1
fi

JUPYTER_URL="http://127.0.0.1:8888/?token=$TOKEN"

echo "JupyterLab está rodando em:"
echo $JUPYTER_URL
