#include "blogel_pagerank_vorPart.h"

int main(int argc, char* argv[])
{
    init_workers();
    blogel_pagerank_vorPart("/data/webuk", "/vor/webuk");
    worker_finalize();
    return 0;
}
