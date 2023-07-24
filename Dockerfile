FROM apache/airflow:2.6.3
USER root
RUN apt-get update \
&& apt-get install wget 
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
