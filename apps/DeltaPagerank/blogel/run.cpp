#include "blogel_app_deltapr.h"
int main(int argc, char* argv[])
{
    init_workers();
    blogel_app_deltapr("/vor/euro", "/exp/sssp");
    worker_finalize();
    return 0;
}
