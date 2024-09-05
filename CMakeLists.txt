cmake_minimum_required(VERSION 3.22)
project(hadoop_testing)

# Set the C++ standard to C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Add the path to the Hadoop include directory
include_directories(/usr/local/libhdfs3/include/hdfs/)
include_directories(/usr/local/folly/include/)
include_directories(/usr/include/boost)

# Add the path to the Hadoop library directory
link_libraries(/usr/local/libhdfs3/lib/libhdfs3.so)

# Add the executable target
add_executable(hadoop_testing main.cpp)

# Link the hdfs and jvm libraries
target_link_libraries(hadoop_testing )
