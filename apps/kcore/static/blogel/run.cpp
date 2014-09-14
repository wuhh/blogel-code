#include "blogel_app_kcore.h"

int main(int argc, char* argv[])
{
    init_workers();
    blogel_app_kcore(argv[1], argv[2]);
    worker_finalize();
    return 0;
}
