FROM eclipse-temurin:21-jdk-alpine AS builder
WORKDIR /workspace

COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
COPY domain/pom.xml domain/pom.xml
COPY application/pom.xml application/pom.xml
COPY adapter-in-rest/pom.xml adapter-in-rest/pom.xml
COPY adapter-in-messaging/pom.xml adapter-in-messaging/pom.xml
COPY adapter-out-persistence/pom.xml adapter-out-persistence/pom.xml
COPY adapter-out-messaging/pom.xml adapter-out-messaging/pom.xml
COPY app/pom.xml app/pom.xml

RUN ./mvnw -B dependency:go-offline || true

COPY domain/src domain/src
COPY application/src application/src
COPY adapter-in-rest/src adapter-in-rest/src
COPY adapter-in-messaging/src adapter-in-messaging/src
COPY adapter-out-persistence/src adapter-out-persistence/src
COPY adapter-out-messaging/src adapter-out-messaging/src
COPY app/src app/src

RUN ./mvnw -B -DskipTests package

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

RUN addgroup -S appgroup && adduser -S appuser -G appgroup
COPY --from=builder /workspace/app/target/app-*.jar /app/ledger-stream.jar
RUN chown -R appuser:appgroup /app

USER appuser

ENV JAVA_OPTS="-XX:MaxRAMPercentage=65.0 -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError"
EXPOSE 8080

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/ledger-stream.jar"]
