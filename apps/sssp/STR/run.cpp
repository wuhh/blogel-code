#include "blogel_sssp_STRRnd1.h"
#include "blogel_sssp_STRRnd2.h"

int main(int argc, char* argv[])
{
    init_workers();
    blogel_sssp_STRRnd1("", "");
    blogel_sssp_STRRnd2("", "");
    worker_finalize();
    return 0;
}
