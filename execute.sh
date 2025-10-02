#!/bin/bash

# Variables de entorno requeridas (ajusta según tu entorno local)
# Contenedor MySQL: --name ingesta-db (root password: secret) | Puerto publicado en host: 4567
# IMPORTANTE: el nombre del contenedor (ingesta-db) NO es el nombre de la base de datos.
MYSQL_HOST=127.0.0.1
MYSQL_PORT=4567
# Ojo: "ingesta-db" es el NOMBRE DEL CONTENEDOR, no el nombre de la base.
# La base que creaste dentro de MySQL es "mi_base"; el usuario que usaremos ahora es root.
MYSQL_USER=root
MYSQL_PASSWORD=secret
MYSQL_DATABASE=mi_base
MYSQL_TABLE=mi_tabla
OUTPUT_CSV=/app/data.csv
S3_BUCKET=ingesta2
S3_OBJECT_KEY=data_csv/data.csv
AWS_DEFAULT_REGION=us-east-1

# Ejecutar el contenedor
# Nota: boto3 tomará credenciales automáticamente desde:
#  - IAM Role si corres en EC2
#  - o ~/.aws/credentials montado como volumen si lo deseas

docker run --rm \
  -e MYSQL_HOST=$MYSQL_HOST \
  -e MYSQL_PORT=$MYSQL_PORT \
  -e MYSQL_USER=$MYSQL_USER \
  -e MYSQL_PASSWORD=$MYSQL_PASSWORD \
  -e MYSQL_DATABASE=$MYSQL_DATABASE \
  -e MYSQL_TABLE=$MYSQL_TABLE \
  -e OUTPUT_CSV=$OUTPUT_CSV \
  -e S3_BUCKET=$S3_BUCKET \
  -e S3_OBJECT_KEY=$S3_OBJECT_KEY \
  -e AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION \
  ingesta