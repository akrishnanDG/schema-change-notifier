# Stage 1: Build
FROM eclipse-temurin:17-jdk AS build
RUN apt-get update && apt-get install -y maven && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn clean package -DskipTests -B

# Stage 2: Runtime
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app

RUN addgroup -S appgroup && adduser -S appuser -G appgroup
RUN mkdir -p /app/config /app/logs && chown -R appuser:appgroup /app

COPY --from=build /app/target/schema-change-notifier-*.jar /app/schema-change-notifier.jar
COPY config/sample-config.properties /app/config/sample-config.properties

USER appuser

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:8080/health || exit 1

ENTRYPOINT ["java", "-jar", "/app/schema-change-notifier.jar"]
CMD ["--config", "/app/config/application.properties", "--mode", "STREAM"]
