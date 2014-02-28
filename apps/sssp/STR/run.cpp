#include "blogel_sssp_STRRnd1.h"
#include "blogel_sssp_STRRnd2.h"

int main(int argc, char* argv[])
{
    init_workers();
    blogel_sssp_STRRnd1("/pullgel/usaxy", "/str1/usaxy_1");
    blogel_sssp_STRRnd2("/str1/usaxy_1", "/str1/usaxy_2");
    worker_finalize();
    return 0;
}
