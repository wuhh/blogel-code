#include "blogel_app_reach.h"

int main(int argc, char* argv[])
{
    init_workers();
    blogel_app_reach("","");
    worker_finalize();
    return 0;
}
