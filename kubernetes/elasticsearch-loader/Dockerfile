FROM python:3.7-bullseye
RUN apt-get update
COPY . /app
RUN cd /app && \
        pip install -e .[tests]

RUN pip install python-snappy
RUN pip install 'elasticsearch-loader[parquet]'
#  elasticsearch_loader --index test --es-host http://host.docker.internal:30001 parquet parquet-data/9.5.parquet 