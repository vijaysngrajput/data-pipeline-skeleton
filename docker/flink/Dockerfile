FROM flink:1.18

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip default-jdk && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt