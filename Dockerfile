FROM dependencies

# Set up the working directory
WORKDIR /app

# Copy the application files
COPY CMakeLists.txt /app/
COPY main.cpp /app/

# Create a build directory and set it as the working directory
RUN mkdir -p /app/build && cd /app/build && \
    cmake .. && make

# Set the default command
CMD ["/app/build/hadoop_testing"]
