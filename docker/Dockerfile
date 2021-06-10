FROM openjdk:11.0.1-jre-slim-sid

ARG CASSPOKE_VERSION=1.3.3

RUN mkdir -p /opt/casspoke
ADD https://github.com/Yelp/dumb-init/releases/download/v1.2.2/dumb-init_1.2.2_amd64 /sbin/dumb-init
ADD https://github.com/criteo/casspoke/releases/download/${CASSPOKE_VERSION}/casspoke-${CASSPOKE_VERSION}.jar /opt/casspoke/casspoke.jar
ADD config.yml /opt/casspoke/
ADD docker/run.sh /

RUN chmod +x /sbin/dumb-init ; \
    chmod 444 /opt/casspoke/*

EXPOSE 8080
CMD ["/sbin/dumb-init", "/bin/bash", "/run.sh"]
