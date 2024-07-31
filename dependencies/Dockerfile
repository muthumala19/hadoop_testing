# Use Ubuntu Jammy as the base image
FROM ubuntu:jammy

# Set environment variable to avoid interactive prompts during package installations
ENV DEBIAN_FRONTEND=noninteractive

# Update package lists and install dependencies in a single RUN command
RUN apt-get update && apt-get install --no-install-recommends -y \
    git \
    g++ \
    make \
    wget \
    curl \
    vim \
    unzip \
    cmake \
    libboost-all-dev \
    libprotobuf-dev \
    protobuf-compiler \
    libxml2-dev \
    krb5-user \
    libkrb5-dev \
    uuid-dev \
    libssl-dev \
    libgsasl7-dev && \
    # Clean up
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /home/ubuntu
RUN mkdir software
WORKDIR /home/ubuntu/software

# Clone the libhdfs3 repository from GitHub
RUN git clone --single-branch --depth 1 -c http.sslVerify=false https://github.com/erikmuttersbach/libhdfs3.git

# Purge git to reduce image size
RUN apt-get purge -y --autoremove git

# Build and install libhdfs3
RUN mkdir /home/ubuntu/software/libhdfs3/build
WORKDIR /home/ubuntu/software/libhdfs3/build
RUN ../bootstrap --prefix=/usr/local/libhdfs3
RUN make -j8
RUN make install

# Clean up
RUN rm -rf /home/ubuntu/software/*