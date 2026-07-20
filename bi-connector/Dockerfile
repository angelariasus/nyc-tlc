FROM ubuntu:22.04

# Evitar prompts durante la instalación
ENV DEBIAN_FRONTEND=noninteractive

# Instalar dependencias necesarias
RUN apt-get update && apt-get install -y wget libssl3 && rm -rf /var/lib/apt/lists/*

# Descargar el conector BI OFICIAL directamente desde los servidores de MongoDB
RUN wget https://info-mongodb-com.s3.amazonaws.com/mongodb-bi/v2/mongodb-bi-linux-x86_64-ubuntu2204-v2.14.9.tgz -O /tmp/bi-connector.tgz

# Extraer e instalar el binario mongosqld
RUN tar -xzf /tmp/bi-connector.tgz -C /tmp/ && \
    cp /tmp/mongodb-bi-linux-x86_64-ubuntu2204-v2.14.9/bin/mongosqld /usr/local/bin/ && \
    rm -rf /tmp/mongodb-bi*

EXPOSE 3307

ENTRYPOINT ["mongosqld"]
