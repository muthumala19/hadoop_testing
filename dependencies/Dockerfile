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
    libgsasl7-dev \
    build-essential \
    linux-generic \
    libmpich-dev \
    libopenmpi-dev \
    libmetis-dev \
    libparmetis-dev \
    ca-certificates && \
    # Clean up
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /home/ubuntu
RUN mkdir software
WORKDIR /home/ubuntu/software

# Clone the libhdfs3 repository from GitHub
RUN git clone --single-branch --depth 1 -c http.sslVerify=false https://github.com/erikmuttersbach/libhdfs3.git

# Build and install libhdfs3
RUN mkdir /home/ubuntu/software/libhdfs3/build
WORKDIR /home/ubuntu/software/libhdfs3/build
RUN ../bootstrap --prefix=/usr/local/libhdfs3
RUN make -j8
RUN make install

# Download and build GKlib (required by ParMETIS)
WORKDIR /home/ubuntu/software
RUN git clone https://github.com/KarypisLab/GKlib.git
WORKDIR /home/ubuntu/software/GKlib
RUN make config prefix=/usr/local/gklib
RUN make install

# Download and build METIS
WORKDIR /home/ubuntu/software
RUN git clone https://github.com/KarypisLab/METIS.git
WORKDIR /home/ubuntu/software/METIS
RUN make config prefix=/usr/local/metis gklib_path=/usr/local/gklib
RUN make install

# Download and build ParMETIS
WORKDIR /home/ubuntu/software
RUN git clone https://github.com/KarypisLab/ParMETIS.git
WORKDIR /home/ubuntu/software/ParMETIS
RUN make config cc=mpicc prefix=/usr/local/parmetis metis_path=/usr/local/metis gklib_path=/usr/local/gklib
RUN make install

# Clean up
RUN rm -rf /home/ubuntu/software/*

# Update library cache
RUN ldconfig

# Purge git to reduce image size
RUN apt-get purge -y --autoremove git

# Set the working directory back to /home/ubuntu
WORKDIR /home/ubuntu
