# AIrflowDockerLocalExecutor
This repository provides an example on how to quickly setup Airflow with 
Local Executor and PostgreSQL as MetaDB for local development.


## Setup
1. Clone repositroy
2. Create **.env** file as a copy of **.env.example**, modify to your liking
3. Add to **requirements.txt** file all Python libraries you need
4. Execute `docker compose up -d`
4. Wait until you see similar entry in the log of PostgreSQL container: `LOG:  database system is ready to accept connections`
5. If Airflow container exited, restart it
6. Wait until you see similar entries in Airflow container log:

```
[INFO] Listening at: http://0.0.0.0:8000 (32)
... [INFO] Using worker: sync
... [INFO] Booting worker with pid: 222
... [223] [INFO] Booting worker with pid: 223
... [224] [INFO] Booting worker with pid: 224
... [225] [INFO] Booting worker with pid: 225
```

7. Go to http://localhost:8000/ (port is defined by **AIRFLOW_UI_PORT** in **.env**) and log in. Username and password are both *admin*
8. Create your DAGs and put them to **./dags** folder
9. Optionally if you want to look inside MetaDB, connect to *localhost:${POSTGRES_PORT}/airflow* via DB client and use airflow as both username and password