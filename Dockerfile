# # Use an official openjdk image that matches your Java 17 environment
# FROM openjdk:17-slim-buster

# # Install curl and gnupg to setup SBT inside our container
# RUN apt-get update && apt-get install -y curl gnupg && \
#     echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
#     echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
#     curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add - && \
#     apt-get update && apt-get install -y sbt

# # Set the working directory inside the virtual container
# WORKDIR /app

# # Copy project definition files first (helps cache dependencies)
# COPY build.sbt /app/
# COPY project /app/project

# # Copy the source code and input data sets
# COPY src /app/src
# COPY data/input /app/data/input

# # Set our environment variable to bypass Windows mode
# ENV RUN_ENV=docker

# # Pre-compile the application inside the container image
# RUN sbt compile

# # Set the default entry point to run our Bronze job
# CMD ["sbt", "runMain com.retail.bronze.BronzeJob"]

# Use a highly-cached, standard Ubuntu base image
FROM ubuntu:22.04

# Avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install Java 17, curl, and gnupg
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    curl \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Set up SBT repository and install it
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add - && \
    apt-get update && apt-get install -y sbt

# Set the working directory inside the container
WORKDIR /app

# Copy build configuration definitions
COPY build.sbt /app/
COPY project /app/project

# Copy your code paths and mock datasets
COPY src /app/src
COPY data/input /app/data/input

# Tell Spark provider to activate the Linux/Docker actual file writer
ENV RUN_ENV=docker

# Compile the application inside the image layer
RUN sbt compile

# Default to run the Bronze Job
CMD ["sbt", "runMain com.retail.bronze.BronzeJob"]