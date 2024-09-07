#include <iostream>
#include <hdfs.h>
#include <sstream>
#include <thread>
#include <chrono>
#include <boost/lockfree/queue.hpp>
#include <vector>
#include <mpi.h>
#include <parmetis.h>

constexpr int queueSize = 100; // Size of the Lockfree Queue
const std::string hdfs_server = "10.8.100.246";
const std::string hdfs_port = "9000";
boost::lockfree::queue<std::string*> queue(queueSize);
const std::string endOfStreamMessage = "__END_OF_STREAM__";

struct Edge {
    idx_t src;
    idx_t dest;
};

std::vector<Edge> edges;
std::vector<idx_t> xadj;
std::vector<idx_t> adjncy;

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
                while (!queue.push(new std::string(line))) {
                    // Wait if the queue is full
                }
                edgeCount++;
            }
        }
    }

    // Handle any leftover data as a line if the file does not end with a newline
    if (!leftover.empty()) {
        while (!queue.push(new std::string(leftover))) {
            // Wait if the queue is full
        }
        edgeCount++;
    }

    // Signal the end of the stream
    while (!queue.push(new std::string(endOfStreamMessage))) {
        // Wait if the queue is full
    }

    // Close the file
    hdfsCloseFile(fs, file);
    std::cout << "Finished reading the file." << std::endl;

    auto endTime = std::chrono::high_resolution_clock::now();
    readTime = endTime - startTime;
}

void processQueue(std::chrono::duration<double>& processTime) {
    std::string* linePtr;
    std::istringstream iss;

    auto startTime = std::chrono::high_resolution_clock::now();

    while (true) {
        while (!queue.pop(linePtr)) {
            // Wait if the queue is empty
        }
        std::unique_ptr<std::string> line(linePtr);
        if (*line == endOfStreamMessage) {
            break; // Exit the loop if end of stream is reached
        }

        // Process the line: parse source and destination vertices
        iss.clear();
        iss.str(*line);
        Edge edge;
        if (iss >> edge.src >> edge.dest) {
            edges.push_back(edge);
        }
    }

    // End timing the processing
    auto endTime = std::chrono::high_resolution_clock::now();
    processTime = endTime - startTime;
}

void convertToCSR() {
    std::vector<std::vector<idx_t>> adjList(edges.size());

    for (const auto& edge : edges) {
        adjList[edge.src].push_back(edge.dest);
    }

    xadj.push_back(0);
    for (const auto& neighbors : adjList) {
        adjncy.insert(adjncy.end(), neighbors.begin(), neighbors.end());
        xadj.push_back(xadj.back() + neighbors.size());
    }
}

void partitionGraph(int numParts) {
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    idx_t nVertices = xadj.size() - 1;
    idx_t ncon = 1;  // Number of constraints
    idx_t nParts = numParts;

    std::vector<real_t> tpwgts(ncon * nParts, 1.0 / nParts);
    std::vector<real_t> ubvec(ncon, 1.05);  // 5% imbalance tolerance

    idx_t options[METIS_NOPTIONS];
    METIS_SetDefaultOptions(options);
    options[METIS_OPTION_NUMBERING] = 0;

    idx_t edgecut;
    std::vector<idx_t> part(nVertices);

    idx_t* vwgt = NULL;
    idx_t* adjwgt = NULL;
    idx_t wgtflag = 0;
    idx_t numflag = 0;

    MPI_Comm comm = MPI_COMM_WORLD;
    int ret = ParMETIS_V3_PartKway(
            xadj.data(), adjncy.data(), vwgt, adjwgt, &wgtflag, &numflag, &ncon,
            &nVertices, &nParts, tpwgts.data(), ubvec.data(), options,
            &edgecut, part.data(), &comm
    );

    if (ret != METIS_OK) {
        std::cerr << "ParMETIS partitioning failed." << std::endl;
    } else {
        std::cout << "ParMETIS partitioning successful." << std::endl;
        std::cout << "Edge cut: " << edgecut << std::endl;
        // Here you can process or output the partitioning results
        for (idx_t i = 0; i < nVertices; ++i) {
            std::cout << "Vertex " << i << " assigned to partition " << part[i] << std::endl;
        }
    }
}


int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);

    // Connect to HDFS
    std::cout << "Connecting to HDFS server " << hdfs_server << " on port " << hdfs_port << "..." << std::endl;
    hdfsFS fs = hdfsConnect(hdfs_server.c_str(), std::stoi(hdfs_port));
    if (!fs) {
        std::cerr << "Failed to connect to HDFS" << std::endl;
        MPI_Finalize();
        return 1;
    }
    std::cout << "Successfully connected to HDFS server." << std::endl;

    // Prompt for file path
    std::string filePath;
    std::cout << "Enter the HDFS file path: ";
    std::getline(std::cin, filePath);

    if (filePath.empty()) {
        std::cerr << "No file path provided. Exiting." << std::endl;
        hdfsDisconnect(fs);
        MPI_Finalize();
        return 1;
    }

    std::chrono::duration<double> readTime, processTime;
    int edgeCount = 0;

    auto totalStartTime = std::chrono::high_resolution_clock::now();

    // Start a thread to read the file line by line (Producer)
    std::thread producerThread(readLinesFromHDFS, fs, filePath, std::ref(readTime), std::ref(edgeCount));

    // Start a thread to process the queue (Consumer)
    std::thread consumerThread(processQueue, std::ref(processTime));

    // Join the producer and consumer threads
    producerThread.join();
    consumerThread.join();

    // Convert the edge list to CSR format
    convertToCSR();

    // Partition the graph
    int numParts = 4; // You can change this to the desired number of partitions
    partitionGraph(numParts);

    // End timing the total process
    auto totalEndTime = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> totalTime = totalEndTime - totalStartTime;

    // Disconnect from HDFS
    hdfsDisconnect(fs);
    std::cout << "Disconnected from HDFS server." << std::endl;

    std::cout << "Time taken for reading: " << readTime.count() << " seconds" << std::endl;
    std::cout << "Time taken for processing: " << processTime.count() << " seconds" << std::endl;
    std::cout << "Total time taken: " << totalTime.count() << " seconds" << std::endl;
    std::cout << "Total edges read: " << edgeCount << std::endl;

    MPI_Finalize();
    return 0;
}
