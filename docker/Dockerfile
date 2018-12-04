FROM openjdk:11.0.1-jre-slim-sid

ARG CASSPOKE_VERSION=1.3
ARG CASSPOKE_SHA512=8b5acbe3df57d0309d7e1c2ceabdfb134e87a7d188a99f9e827c1910a05fb48782811fbf5cd9a6ed01201118001e74fc4450409d3898eb4833c5cfc02bde8845

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
