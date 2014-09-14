#include "blogel_kcore_vorPart.h"

int main(int argc, char* argv[])
{
    init_workers();
    blogel_kcore_vorPart(argv[1], argv[2]);
    worker_finalize();
    return 0;
}
