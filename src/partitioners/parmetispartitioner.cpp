#include "parmetispartitioner.h"
#include <parmetis.h>
#include <mpi.h>
#include <iostream>

ParMetisPartitioner::ParMetisPartitioner(int numParts)
        : numPartitions(numParts) {
    // Initialize empty graph
}

ParMetisPartitioner::~ParMetisPartitioner() {
}

void ParMetisPartitioner::addEdge(int vertexA, int vertexB) {
    adjncy.push_back(vertexB); // Add an edge between vertexA and vertexB
}

void ParMetisPartitioner::convertToCSR() {
    // The xadj array stores where each vertex's adjacency list starts
    int edgeIndex = 0;
    for (size_t i = 0; i < adjncy.size(); ++i) {
        xadj.push_back(edgeIndex);
        edgeIndex += adjncy[i];
    }
}

void ParMetisPartitioner::partitionGraph() {
    convertToCSR();  // Prepare the data in CSR format

    int numVertices = xadj.size() - 1;
    partitionResult.resize(numVertices);

    int options[3] = {0, 0, 0};
    int edgeCut; // Stores the number of edges cut by the partition
    int wgtflag = 0, numflag = 0;
    int ncon = 1; // Number of balancing constraints (typically 1)
    real_t *tpwgts = new real_t[numPartitions * ncon]; // Target partition weights

    for (int i = 0; i < numPartitions * ncon; ++i) {
        tpwgts[i] = 1.0 / numPartitions; // Equal partition weights
    }

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // Call ParMETIS to partition the graph
    int result = ParMETIS_V3_PartKway(&xadj[0], &adjncy[0], nullptr, nullptr,
                                      &wgtflag, &numflag, &ncon, &numPartitions,
                                      tpwgts, nullptr, options, &edgeCut,
                                      &partitionResult[0], MPI_COMM_WORLD);

    if (result != METIS_OK) {
        std::cerr << "ParMETIS partitioning failed!" << std::endl;
    } else {
        std::cout << "Partitioning complete with " << edgeCut << " edge cuts." << std::endl;
    }

    delete[] tpwgts;
}

void ParMetisPartitioner::displayPartitionResults() {
    std::cout << "Vertex partition results:" << std::endl;
    for (size_t i = 0; i < partitionResult.size(); ++i) {
        std::cout << "Vertex " << i << " is in partition " << partitionResult[i] << std::endl;
    }
}
