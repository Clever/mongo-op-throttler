FROM google/debian:wheezy

RUN apt-get -y update && apt-get -y install curl
RUN curl -L https://github.com/Clever/gearcmd/releases/download/v0.5.1/gearcmd-v0.5.1-linux-amd64.tar.gz | tar xz -C /usr/local/bin --strip-components 1

COPY bin/mongo-op-throttler /usr/bin/mongo-op-throttler

CMD ["gearcmd", "--name", "mongo-op-throttler", "--cmd", "/usr/bin/mongo-op-throttler"]

