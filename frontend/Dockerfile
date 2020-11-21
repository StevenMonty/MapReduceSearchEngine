FROM openjdk:8-jdk-alpine
ENV LANG en_GB.UTF-8

ENV DISPLAY=10.0.0.242:0
ENV ASSET_PATH="srcFiles.txt"
ENV GOOGLE_APPLICATION_CREDENTIALS="credentials/DHFS_Cluster_Auth.json"
ENV PROJECT_ID="463246640870"
ENV BUCKET_NAME="cloud-comp-final-proj-data"

#RUN apk add fontconfig

RUN apk add --update ttf-dejavu && rm -rf /var/cache/apk/*

#docker rmi $(docker images -f dangling=true -q)

#RUN ls /
COPY target/SearchEngineGUI.jar /SearchEngineGUI.jar
COPY src/main/resources/srcFiles.txt /srcFiles.txt
COPY src/main/resources/credentials /credentials

WORKDIR /
#RUN ls

ENTRYPOINT ["java", "-jar", "SearchEngineGUI.jar"]
#ENTRYPOINT ["java", "SearchEngineGUI"]
