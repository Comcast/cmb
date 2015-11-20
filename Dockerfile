FROM maven:3.3.3-jdk-8

RUN mkdir -p /app/src/cmb
ADD . /app/src/cmb

WORKDIR /app/src/cmb
ENV ROOTDIR=/app/src/cmb
RUN mvn -Dmaven.test.skip=true assembly:assembly
RUN mv target/lib $ROOTDIR/lib
RUN mv target/cmb.jar $ROOTDIR/lib

EXPOSE 6059 6061

CMD $ROOTDIR/docker/cmb/bin/start.sh
