# syntax=docker/dockerfile:1
FROM gradle:8.12-jdk21 AS build
WORKDIR /workspace
COPY . .
RUN gradle :backend:bootJar --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/backend/build/libs/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
