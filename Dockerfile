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
COPY load-generator/pom.xml load-generator/pom.xml

RUN ./mvnw -B -pl app -am dependency:go-offline || true

COPY domain/src domain/src
COPY application/src application/src
COPY adapter-in-rest/src adapter-in-rest/src
COPY adapter-in-messaging/src adapter-in-messaging/src
COPY adapter-out-persistence/src adapter-out-persistence/src
COPY adapter-out-messaging/src adapter-out-messaging/src
COPY app/src app/src

RUN ./mvnw -B -pl app -am -DskipTests package

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

RUN addgroup -S appgroup && adduser -S appuser -G appgroup
COPY --from=builder /workspace/app/target/app-*.jar /app/ledger-stream.jar
RUN chown -R appuser:appgroup /app

USER appuser

ENV JAVA_OPTS="-XX:MaxRAMPercentage=60.0 -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError"
EXPOSE 8080

# Forward SIGTERM/SIGINT to the JVM so Spring Boot graceful shutdown runs cleanly.
# Translate JVM's normal SIGTERM exit code (143 = 128 + 15) to 0 so Kubernetes records
# reason=Completed instead of Error while preserving genuine non-zero crash exit codes.
ENTRYPOINT ["sh", "-c", "trap 'kill -TERM $PID; wait $PID; EC=$?; [ \"$EC\" -eq 143 ] && exit 0 || exit \"$EC\"' TERM INT; java $JAVA_OPTS -jar /app/ledger-stream.jar & PID=$!; wait $PID"]
