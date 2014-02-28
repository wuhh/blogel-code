#include "blogel_pagerank_urlPart.h"

int main(int argc, char* argv[])
{
    init_workers();
    blogel_pagerank_urlPart("/data/webbase", "/url/webbase");
    worker_finalize();
    return 0;
}
