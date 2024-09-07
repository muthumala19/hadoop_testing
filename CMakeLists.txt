cmake_minimum_required(VERSION 3.22)
project(hadoop_testing)

# Set the C++ standard to C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Define variables for customizable paths
set(LIBHDFS3_INCLUDE_DIR "/usr/local/libhdfs3/include/hdfs/")
set(FOLLY_INCLUDE_DIR "/usr/local/folly/include/")
set(BOOST_INCLUDE_DIR "/usr/include/boost")
set(PARMETIS_INCLUDE_DIR "/usr/local/parmetis/include/")
set(PARMETIS_BIN_DIR "/usr/local/parmetis/bin/")

set(LIBHDFS3_LIB "/usr/local/libhdfs3/lib/libhdfs3.so")
set(LIBPARMETIS_LIB "/usr/lib/libparmetis.so")
set(LIBMETIS_LIB "/usr/lib/x86_64-linux-gnu/libmetis.so")

# Include directories
include_directories(${LIBHDFS3_INCLUDE_DIR})
include_directories(${FOLLY_INCLUDE_DIR})
include_directories(${BOOST_INCLUDE_DIR})
include_directories(${PARMETIS_INCLUDE_DIR})

# Add the executable target
add_executable(hadoop_testing main.cpp)

# Link against necessary libraries
target_link_libraries(hadoop_testing ${LIBHDFS3_LIB} ${LIBPARMETIS_LIB} ${LIBMETIS_LIB})

# Find MPI and include it
find_package(MPI REQUIRED)
include_directories(${MPI_INCLUDE_PATH})
target_link_libraries(hadoop_testing ${MPI_LIBRARIES})