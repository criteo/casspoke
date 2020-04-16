FROM openjdk:11.0.1-jre-slim-sid

ARG CASSPOKE_VERSION=1.3.1
ARG CASSPOKE_SHA512=eca9280a70af3c556f46d33bb15de4b63f3e04b67a13fc25b53119d5afe3ad8d12a82d5cf8be7df67eaf3ba2791df715d2787c575ecef644550046a2c52a97a9

RUN mkdir -p /opt/casspoke
ADD https://github.com/Yelp/dumb-init/releases/download/v1.2.2/dumb-init_1.2.2_amd64 /sbin/dumb-init
ADD https://github.com/criteo/casspoke/releases/download/${CASSPOKE_VERSION}/casspoke-${CASSPOKE_VERSION}-all.jar /opt/casspoke/casspoke.jar
RUN echo "${CASSPOKE_SHA512} /opt/casspoke/casspoke.jar" > sha512_checksum.txt && sha512sum -c sha512_checksum.txt
ADD config.yml /opt/casspoke/
ADD run.sh /

RUN chmod +x /sbin/dumb-init ; \
    chmod 444 /opt/casspoke/*

EXPOSE 8080
CMD ["/sbin/dumb-init", "/bin/bash", "/run.sh"]
