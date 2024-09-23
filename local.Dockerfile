FROM apache/airflow:2.9.1
ARG AIRFLOW_HOME=/opt/airflow
USER airflow

COPY ../requirements.txt ${AIRFLOW_HOME}/requirements.txt
RUN pip install --upgrade pip

RUN pip install  -r  ${AIRFLOW_HOME}/requirements.txt

EXPOSE 8000 5555 8793

WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]