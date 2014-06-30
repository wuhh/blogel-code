#include "blogel_kcore_vorPart.h"

int main(int argc, char* argv[])
{
    init_workers();
    blogel_kcore_vorPart("/temp/dblp", "/vor/dblp");
    worker_finalize();
    return 0;
}
