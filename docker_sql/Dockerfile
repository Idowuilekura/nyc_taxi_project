FROM python:3.9-slim 

WORKDIR /app

COPY ./docker_sql/etl_script /app/etl_script

COPY ./docker_sql/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt 

EXPOSE 8090

ENTRYPOINT [ "python","/app/etl_script/ny_run_etl_pipeline.py" ]

# COPY .test/ /app/test 




