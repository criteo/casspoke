FROM openjdk:11.0.1-jre-slim-sid

ARG CASSPOKE_VERSION=1.2
ARG CASSPOKE_SHA512=7f35e402aa82996bf5ac9591e33d9254a638dcedb19ff8dc80be03a8ff4c95cbd77a996ee164a47a94376272867b35d27694d235366c01e14223d50f9d954141

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
