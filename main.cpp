#include <iostream>
#include <hdfs.h> // Ensure hdfs.h is in the include path
#include <sstream>

void readLinesFromHDFS(hdfsFS fs, const std::string &filePath) {
    const int bufferSize = 4096; // Size of the buffer for reading chunks
    char buffer[bufferSize];

    // Open the HDFS file
    hdfsFile file = hdfsOpenFile(fs, filePath.c_str(), O_RDONLY, 0, 0, 0);
    if (!file) {
        std::cerr << "Failed to open file for reading: " << filePath << std::endl;
        return;
    }
    std::cout << "Successfully opened file: " << filePath << std::endl;

    std::string leftover; // To store leftover data from buffer reads
    tSize bytesRead;

    // Read chunks of data from the file
    while ((bytesRead = hdfsRead(fs, file, buffer, bufferSize)) > 0) {
        std::string data(buffer, bytesRead);
        leftover += data; // Append data to any leftover from the previous read

        std::istringstream stream(leftover);
        std::string line;
        leftover.clear(); // Clear leftover since we are processing it

        // Process each line in the current buffer
        while (std::getline(stream, line)) {
            if (stream.eof() && data.back() != '\n') {
                // If a line is incomplete (no newline at the end), store it as leftover
                leftover = line;
            } else {
                // Process the complete line
                std::cout << line << std::endl;
            }
        }
    }

    // Handle any leftover data as a line if the file does not end with a newline
    if (!leftover.empty()) {
        std::cout << leftover << std::endl;
    }

    // Close the file
    hdfsCloseFile(fs, file);
    std::cout << "Finished reading the file." << std::endl;
}

int main() {
    // Prompt the user for the file path
    std::string filePath;
    std::cout << "Enter the HDFS file path: ";
    std::getline(std::cin, filePath);

    // Check if the user entered a path
    if (filePath.empty()) {
        std::cerr << "No file path provided. Exiting." << std::endl;
        return 1;
    }

    // Connect to HDFS
    std::cout << "Connecting to HDFS server at 127.0.0.1:9000..." << std::endl;
    hdfsFS fs = hdfsConnect("127.0.0.1", 9000);
    if (!fs) {
        std::cerr << "Failed to connect to HDFS" << std::endl;
        return 1;
    }
    std::cout << "Successfully connected to HDFS server." << std::endl;

    // Read the file line by line
    readLinesFromHDFS(fs, filePath);

    // Disconnect from HDFS
    hdfsDisconnect(fs);
    std::cout << "Disconnected from HDFS server." << std::endl;

    return 0;
}
