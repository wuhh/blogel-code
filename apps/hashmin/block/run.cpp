#include "blogel_app_hashmin.h"

int main(int argc, char* argv[])
{
    init_workers();
    blogel_app_hashmin("/str/usaxy_2", "/exp/cc");
    worker_finalize();
    return 0;
}
