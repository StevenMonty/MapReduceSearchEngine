FROM openjdk:8-jdk-alpine
ENV LANG en_GB.UTF-8

ENV DISPLAY=10.0.0.242:0
ENV ASSET_PATH="assets"
ENV GOOGLE_APPLICATION_CREDENTIALS="credentials/DHFS_Cluster_Auth.json"
#RUN apk add fontconfig

RUN apk add --update ttf-dejavu && rm -rf /var/cache/apk/*

#docker rmi $(docker images -f dangling=true -q)

RUN ls /
COPY target/SearchEngineGUI-1.0-jar-with-dependencies.jar /SearchEngineGUI.jar
COPY src/main/resources/assets /assets
COPY src/main/resources/credentials /credentials

WORKDIR /
RUN ls

ENTRYPOINT ["java", "-jar", "SearchEngineGUI.jar"]
#ENTRYPOINT ["java", "SearchEngineGUI"]
