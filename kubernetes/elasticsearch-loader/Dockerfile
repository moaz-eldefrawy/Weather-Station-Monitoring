FROM python:3.7-bullseye
RUN apt-get update
COPY . /app
RUN cd /app && \
        pip install -e .[tests]

RUN pip install python-snappy
RUN pip install 'elasticsearch-loader[parquet]'
RUN pip install watchdog
CMD ["python", "/app/watch.py"]
