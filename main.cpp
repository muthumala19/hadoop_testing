#include <iostream>
#include <hdfs.h>
#include <sstream>
#include <folly/ProducerConsumerQueue.h>
#include <thread>
#include <chrono> // For timing

constexpr int queueSize = 100; // Size of the ProducerConsumerQueue
folly::ProducerConsumerQueue<std::string> queue(queueSize);
const std::string endOfStreamMessage = "__END_OF_STREAM__";

void readLinesFromHDFS(hdfsFS fs, const std::string &filePath, std::chrono::duration<double>& readTime, int& edgeCount) {
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

    // Start timing the reading process
    auto startTime = std::chrono::high_resolution_clock::now();

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
                // Enqueue the complete line
                while (!queue.write(line)) {
                    // Wait if the queue is full
                }
                edgeCount++;
            }
        }
    }

    // Handle any leftover data as a line if the file does not end with a newline
    if (!leftover.empty()) {
        while (!queue.write(leftover)) {
            // Wait if the queue is full
        }
        edgeCount++;
    }

    // Signal the end of the stream
    while (!queue.write(endOfStreamMessage)) {
        // Wait if the queue is full
    }

    // Close the file
    hdfsCloseFile(fs, file);
    std::cout << "Finished reading the file." << std::endl;

    // End timing the reading process
    auto endTime = std::chrono::high_resolution_clock::now();
    readTime = endTime - startTime;
}

void processQueue(std::chrono::duration<double>& processTime) {
    std::string line;

    // Start timing the processing
    auto startTime = std::chrono::high_resolution_clock::now();


    while (true) {
        while (!queue.read(line)) {
            // Wait if the queue is empty
        }
        if (line == endOfStreamMessage) {
            break; // Exit the loop if end of stream is reached
        }
        // Process the line
//        std::cout << line << std::endl;
    }

    // End timing the processing
    auto endTime = std::chrono::high_resolution_clock::now();
    processTime = endTime - startTime;
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

    // Variables to store the elapsed time and edge count
    std::chrono::duration<double> readTime, processTime;
    int edgeCount = 0;

    // Start timing the total process
    auto totalStartTime = std::chrono::high_resolution_clock::now();

    // Start a thread to read the file line by line (Producer)
    std::thread producerThread(readLinesFromHDFS, fs, filePath, std::ref(readTime), std::ref(edgeCount));

    // Start a thread to process the queue (Consumer)
    std::thread consumerThread(processQueue, std::ref(processTime));

    // Join the producer and consumer threads
    producerThread.join();
    consumerThread.join();

    // End timing the total process
    auto totalEndTime = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> totalTime = totalEndTime - totalStartTime;

    // Disconnect from HDFS
    hdfsDisconnect(fs);
    std::cout << "Disconnected from HDFS server." << std::endl;

    // Print the elapsed time and edge count
    std::cout << "Time taken for reading: " << readTime.count() << " seconds" << std::endl;
    std::cout << "Time taken for processing: " << processTime.count() << " seconds" << std::endl;
    std::cout << "Total time taken: " << totalTime.count() << " seconds" << std::endl;
    std::cout << "Total edges read: " << edgeCount << std::endl;

    return 0;
}
