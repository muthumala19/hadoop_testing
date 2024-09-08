#include <iostream>
#include <hdfs.h>
#include <sstream>
#include <thread>
#include <chrono>
#include <boost/lockfree/queue.hpp>
#include <vector>
#include <mpi.h>
#include <parmetis.h>
#include <set> // Include for std::set

constexpr int queueSize = 100; // Size of the Lockfree Queue
const std::string hdfs_server = "127.0.0.1";
const std::string hdfs_port = "9000";
boost::lockfree::queue<std::string *> queue(queueSize);
const std::string endOfStreamMessage = "__END_OF_STREAM__";

struct Edge {
    idx_t src;
    idx_t dest;

    bool operator<(const Edge &other) const {
        return std::tie(src, dest) < std::tie(other.src, other.dest);
    }
};

std::vector<idx_t> xadj;
std::vector<idx_t> adjncy;

std::set<idx_t> uniqueVertices;
std::set<Edge> uniqueEdges;
std::vector<std::pair<idx_t, std::vector<idx_t>>> adjacencyList;


void
readLinesFromHDFS(hdfsFS fs, const std::string &filePath, std::chrono::duration<double> &readTime, int &edgeCount) {
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

void processQueue(std::chrono::duration<double> &processTime) {
    std::string *linePtr;
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
            // Add vertices to the set
            uniqueVertices.insert(edge.src);
            uniqueVertices.insert(edge.dest);
            // Insert the edge into the set of unique edges
            uniqueEdges.insert(edge);
        }
    }

    // End timing the processing
    auto endTime = std::chrono::high_resolution_clock::now();
    processTime = endTime - startTime;
}

void convertToCSR() {
    std::cout << "CSR conversion started." << std::endl;

    // Populate the adjacencyList vector
    int verticesCount = uniqueVertices.size();
    adjacencyList.resize(verticesCount);

    for (const auto &edge: uniqueEdges) {
        adjacencyList[edge.src].second.push_back(edge.dest);
    }

    std::cout << "Adjacency list created with " << adjacencyList.size() << " vertices." << std::endl;

    // Generate xadj and adjncy
    xadj.resize(verticesCount + 1, 0); // Note: +1 for CSR format
    adjncy.clear();

    // Fill the xadj and adjncy vectors
    for (int i = 0; i < verticesCount; i++) {
        xadj[i + 1] = xadj[i] + adjacencyList[i].second.size(); // Correct CSR construction
        adjncy.insert(adjncy.end(), adjacencyList[i].second.begin(),
                      adjacencyList[i].second.end()); // Append all neighbors
    }

    std::cout << "CSR conversion completed." << std::endl;

}

void partitionGraph(int numParts) {
    std::cout << "Partition started with parmetis" << std::endl;
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    idx_t nVertices = uniqueVertices.size();  // Total number of vertices
    idx_t ncon = 1;  // Number of constraints
    idx_t nparts = numParts;  // Number of partitions should be of type idx_t

    // Corrected type for tpwgts and ubvec
    std::vector<real_t> tpwgts(ncon * nparts, 1.0 / nparts); // Weight distribution for partitions
    std::vector<real_t> ubvec(ncon, 1.05);  // 5% imbalance tolerance

    idx_t options[METIS_NOPTIONS];
    METIS_SetDefaultOptions(options);
    options[METIS_OPTION_NUMBERING] = 0;  // Start numbering from 0

    idx_t edgecut;
    std::vector<idx_t> part(nVertices);

    idx_t *vwgt = NULL;  // No vertex weights
    idx_t *adjwgt = NULL;  // No edge weights
    idx_t wgtflag = 0;  // No weights present
    idx_t numflag = 0;  // C-style numbering

    // Define vtxdist array to distribute vertices across processors
    std::vector<idx_t> vtxdist(size + 1);  // vtxdist has size (number of processors + 1)
    idx_t verticesPerProc = nVertices / size;
    idx_t remainder = nVertices % size;

    for (int i = 0; i < size; ++i) {
        vtxdist[i] = i * verticesPerProc + std::min(static_cast<idx_t>(i), remainder);
    }
    vtxdist[size] = nVertices;  // Last element is the total number of vertices

    MPI_Comm comm = MPI_COMM_WORLD;
    int ret = ParMETIS_V3_PartKway(
            vtxdist.data(),  // Distribution of vertices across processes
            xadj.data(),     // Index of adjacency structure
            adjncy.data(),   // Adjacency list data
            vwgt,            // Vertex weights (NULL)
            adjwgt,          // Edge weights (NULL)
            &wgtflag,        // Indicates no weights
            &numflag,        // C-style numbering
            &ncon,           // Number of balancing constraints
            &nparts,         // Corrected: Number of partitions
            tpwgts.data(),   // Desired weight for each partition and constraint
            ubvec.data(),    // Imbalance tolerance
            options,         // Options array
            &edgecut,        // Stores the edge-cut result
            part.data(),     // Stores the partition assignment for each vertex
            &comm            // Corrected: MPI communicator pointer
    );

    if (ret != METIS_OK) {
        std::cerr << "ParMETIS partitioning failed." << std::endl;
    } else {
        std::cout << "ParMETIS partitioning successful." << std::endl;
        std::cout << "Edge cut: " << edgecut << std::endl;
    }
}

int main(int argc, char *argv[]) {
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
    std::cin >> filePath;

    // Check if file exists
    hdfsFileInfo *fileInfo = hdfsGetPathInfo(fs, filePath.c_str());
    if (!fileInfo) {
        std::cerr << "File does not exist on HDFS: " << filePath << std::endl;
        hdfsDisconnect(fs);
        MPI_Finalize();
        return 1;
    }
    hdfsFreeFileInfo(fileInfo, 1);

    std::chrono::duration<double> readTime, processTime;
    int edgeCount = 0;

    auto totalStartTime = std::chrono::high_resolution_clock::now();

    std::thread producerThread(readLinesFromHDFS, fs, filePath, std::ref(readTime), std::ref(edgeCount));
    std::thread consumerThread(processQueue, std::ref(processTime));

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
    std::cout << "Total edges read: " << uniqueEdges.size() << std::endl;

    // Print the number of unique vertices and edges
    std::cout << "Number of unique vertices: " << uniqueVertices.size() << std::endl;
    std::cout << "Number of unique edges: " << uniqueEdges.size() << std::endl;

    MPI_Finalize();
    return 0;
}
