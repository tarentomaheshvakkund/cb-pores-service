FROM openjdk:8
COPY cb-pores-service-0.0.1-SNAPSHOT.jar /opt/
CMD ["java", "-XX:+PrintFlagsFinal", "-XX:+UnlockExperimentalVMOptions", "-XX:+UseCGroupMemoryLimitForHeap", "-jar", "/opt/cb-pores-service-0.0.1-SNAPSHOT.jar"]
