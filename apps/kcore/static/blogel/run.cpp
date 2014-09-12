#include "blogel_app_kcore.h"

int main(int argc, char* argv[])
{
    init_workers();
    blogel_app_kcore("/vor", "/blogel");
    worker_finalize();
    return 0;
}
