#include "blogel_sssp_vorPart.h"

int main(int argc, char* argv[])
{
    init_workers();
    blogel_sssp_vorPart("", "");
    worker_finalize();
    return 0;
}
