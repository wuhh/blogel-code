#include "blogel_pagerank_vorPart.h"

int main(int argc, char* argv[])
{
    init_workers();
    blogel_pagerank_vorPart("", "");
    worker_finalize();
    return 0;
}
