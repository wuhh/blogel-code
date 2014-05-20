#include "blogel_app_sssp.h"
int main(int argc, char* argv[])
{
    init_workers();
    blogel_app_sssp("/str/usaxy_2", "/exp/blogel");
    worker_finalize();
    return 0;
}
