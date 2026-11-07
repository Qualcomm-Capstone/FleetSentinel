FROM eclipse-temurin:21-jre
WORKDIR /app
COPY build/libs/gateway-0.1.0.jar /app/gateway.jar
USER 10001:10001
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=65", "-XX:+ExitOnOutOfMemoryError", "-jar", "/app/gateway.jar"]
