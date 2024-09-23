#!/usr/bin/env bash

case "$1" in
  webserver)
    sleep 10;
    airflow db init;
    airflow db upgrade;
    airflow users create -u admin -p admin -f admin -l admin --role Admin --email admin@admin.com;
    airflow webserver --port 8000 &
    exec airflow scheduler
    ;;
  *)
    exec "$@"
    ;;
esac