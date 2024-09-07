#ifndef PARMETISPARTITIONER_H
#define PARMETISPARTITIONER_H

#include <vector>

class ParMetisPartitioner {
public:
    ParMetisPartitioner(int numParts);
    ~ParMetisPartitioner();

    void addEdge(int vertexA, int vertexB);  // Add an edge between vertexA and vertexB
    void convertToCSR();                     // Convert edge list to CSR format
    void partitionGraph();                   // Partition the graph using ParMETIS
    void displayPartitionResults();          // Display the results of the partitioning

private:
    int numPartitions;                      // Number of partitions
    std::vector<int> xadj;                  // CSR: index into adjacency list for each vertex
    std::vector<int> adjncy;                // CSR: adjacency list (edge endpoints)
    std::vector<int> partitionResult;       // Partition result for each vertex
};

#endif // PARMETISPARTITIONER_H
