#include "blogel_app_kcore.h"

int main(int argc, char* argv[])
{
    init_workers();
    blogel_app_kcore("/huan/result_syn1", "/huan/syn1_result_blogel");
    worker_finalize();
    return 0;
}
