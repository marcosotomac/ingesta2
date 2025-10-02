

#!/bin/bash

# Variables de entorno requeridas (ajusta según tu entorno local)
MYSQL_HOST=host.docker.internal
MYSQL_PORT=3306
MYSQL_USER=mi_usuario
MYSQL_PASSWORD=mi_password
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