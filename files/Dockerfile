FROM maven:3.6.3-openjdk-14 AS builder

WORKDIR /app
COPY src ./src
COPY pom.xml ./pom.xml
RUN mvn -e -B package

FROM openjdk:14
COPY config.json /
COPY --from=builder /app/target/fragmenter-1.0-SNAPSHOT.jar /
CMD ["java", "-jar", "/fragmenter-1.0-SNAPSHOT.jar"]
