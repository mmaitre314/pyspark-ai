FROM spark:4.0.1-scala2.13-java21-python3-ubuntu

USER root

RUN apt-get update && apt-get install -y git
RUN pip3 install pytest==9.0.2 ipykernel==7.1.0 chispa==0.11.1 delta-spark==4.0.1

ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.9-src.zip
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3
