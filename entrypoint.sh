#!/bin/bash

# Inicializa o banco de dados
airflow db migrate

# Cria usuário admin (altere senha/email conforme necessário)
airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@admin.com \
    --password admin

# Inicia o Scheduler em background (&)
airflow scheduler &

# Inicia o Webserver em foreground (segura o container de pé)
exec airflow webserver