FROM jupyter/pyspark-notebook:latest

USER root
RUN apt-get update && apt-get install -y wget

RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -P /usr/local/spark/jars

USER jovyan
