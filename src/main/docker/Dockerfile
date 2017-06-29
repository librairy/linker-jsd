FROM openjdk:8-jre-alpine
MAINTAINER Librairy <librairy.framework@gmail.com>
VOLUME /tmp
ADD linker-jsd.jar app.jar
#RUN sh -c 'touch /app.jar'
ENTRYPOINT exec java $JAVA_OPTS -server -jar /app.jar