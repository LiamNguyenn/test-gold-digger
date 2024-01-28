# Base Image
FROM --platform=linux/amd64 python:3.10-slim
LABEL maintainer="lam.nguyenbatung"

ARG LAMBDA_HOME=/opt/lambda_linux

ENV LAMBDA_HOME=${LAMBDA_HOME}

# Install dependencies and tools
RUN apt-get update -y && \
    apt-get upgrade -yqq && \
    apt-get install -yqq --no-install-recommends \
    python3-dev \
    wget \
    libczmq-dev \
    curl \
    libssl-dev \
    git \
    inetutils-telnet \
    bind9utils freetds-dev \
    libkrb5-dev \
    libsasl2-dev \
    libffi-dev libpq-dev \
    freetds-bin build-essential \
    default-libmysqlclient-dev \
    apt-utils \
    rsync \
    zip \
    unzip \
    gcc \
    vim \
    locales \
    && apt-get autoremove -yqq --purge && apt-get clean

# Upgrade pip
# Install constraint with subpackages
RUN pip install --upgrade "pip==23.2.1" && \
    useradd -ms /bin/bash -d ${LAMBDA_HOME} lambda_admin && \
    pip install --user --upgrade pip setuptools

# INSTALL AWS CLI
# RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
#     unzip awscliv2.zip && \
#     ./aws/install

# Set the owner of the files in LAMBDA_HOME to the user airflow
RUN chown -R lambda_admin: ${LAMBDA_HOME}

# Copy the entrypoint.sh from host to container (at path LAMBDA_HOME)
#COPY ./start.sh ./start.sh

# Set the entrypoint.sh file to be executable
#RUN chmod +x ./start.sh

# Set the username to use
USER lambda_admin

# Execute start-airflow.sh
# CMD [ "./start.sh" ]
