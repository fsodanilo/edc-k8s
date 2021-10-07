FROM gcr.io/spark-operator/spark-py:v3.0.0
LABEL MAINTAINER Carlos Barbosa 
LABEL EMAIL carlos.barbosa@a3data.com.br

ARG aws_access_key_id
ARG aws_secret_access_key

ENV AWS_ACCESS_KEY_ID $aws_access_key_id
ENV AWS_SECRET_ACCESS_KEY $aws_secret_access_key
ENV AWS_DEFAULT_REGION us-east-2

# using root
USER root:root

# create directory for applications
RUN mkdir -p /app 

#copy jars files
COPY ./jars/ /opt/spark/jars

#set main work directory
WORKDIR /app

#user
USER root