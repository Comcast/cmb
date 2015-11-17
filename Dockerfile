FROM java:8

RUN mkdir -p /app/src/cmb
ADD . /app/src/cmb
WORKDIR /app/src/cmb

EXPOSE 6059 6061

CMD ./docker/cmb/bin/start.sh
