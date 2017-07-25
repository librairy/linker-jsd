FROM frolvlad/alpine-oraclejdk8:slim
MAINTAINER Librairy <librairy.framework@gmail.com>
ENV LIBRAIRY_HOME /librairy
VOLUME $LIBRAIRY_HOME
ADD linker-jsd.jar app.jar
#RUN sh -c 'touch /app.jar'
ENTRYPOINT exec java $JAVA_OPTS -server -jar /app.jar