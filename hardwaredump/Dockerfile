# Stage 1: Build the application with Maven
FROM maven:3.9.4-eclipse-temurin-17-alpine AS build

WORKDIR /build

# Only copy the files needed to build — improves caching!
COPY pom.xml .
COPY src ./src

RUN mvn clean package -DskipTests

# Stage 2: Use a lightweight JDK image for runtime
FROM eclipse-temurin:17-jdk-alpine

WORKDIR /app

# Copy the built JAR from the first stage
COPY --from=build /build/target/hardwaredump-0.0.1-SNAPSHOT.jar app.jar

# Run the app
CMD ["java", "-jar", "app.jar"]
