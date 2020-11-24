FROM openjdk:8-jre-alpine
ENV LANG en_GB.UTF-8

ENV DISPLAY=10.0.0.242:0
ENV FILE_LIST_PATH="srcFiles.txt"
ENV GOOGLE_APPLICATION_CREDENTIALS="credentials/cloud-comp-dhfs-cluster-2df39dc6c2fb.json"
ENV PROJECT_ID="cloud-comp-dhfs-cluster"
ENV BUCKET_NAME="cloud-comp-final-proj-data"
ENV REGION="us-central1"
ENV CLUSTER_NAME="cloud-comp-final-proj-cluster"
ENV BUCKET_ASSET_PATH="cloud-comp-final-proj-data/assets"
ENV JOB_INPUT_DIR="cloud-comp-final-proj-data/input"
ENV JOB_OUTPUT_DIR="cloud-comp-final-proj-data/output"

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
