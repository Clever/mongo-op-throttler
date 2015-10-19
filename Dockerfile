FROM google/debian:wheezy

RUN apt-get -y update && apt-get -y install curl
RUN curl -L https://github.com/Clever/gearcmd/releases/download/v0.3.8/gearcmd-v0.3.8-linux-amd64.tar.gz | tar xz -C /usr/local/bin --strip-components 1

COPY mongo-op-throttler /usr/bin/mongo-op-throttler

CMD ["gearcmd", "--name", "mongo-op-throttler", "--cmd", "/usr/bin/mongo-op-throttler"]

