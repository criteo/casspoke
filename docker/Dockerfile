FROM openjdk:11.0.1-jre-slim-sid

ARG CASSPOKE_VERSION=1.1
ARG CASSPOKE_SHA512=51e7a22e579d159a0cb1f3248318ed588b9bbf57b4f4674e0637c37b463c6cb248c75b7218ce2727d67e206653f2e750c4bedb7c11e3dded79eb35664efc4315

RUN mkdir -p /etc/casspoke /opt/casspoke
ADD https://github.com/Yelp/dumb-init/releases/download/v1.2.2/dumb-init_1.2.2_amd64 /sbin/dumb-init
ADD https://github.com/criteo/casspoke/releases/download/${CASSPOKE_VERSION}/casspoke-${CASSPOKE_VERSION}-all.jar /opt/casspoke/casspoke.jar
RUN echo "${CASSPOKE_SHA512} /opt/casspoke/casspoke.jar" > sha512_checksum.txt && sha512sum -c sha512_checksum.txt
ADD config.yml /etc/casspoke/
ADD run.sh /

RUN chmod +x /sbin/dumb-init

CMD ["/sbin/dumb-init", "/bin/bash", "/run.sh"]
