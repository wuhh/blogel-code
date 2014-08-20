#include "blogel_kcore_vorPart.h"

int main(int argc, char* argv[])
{
    init_workers();
    blogel_kcore_vorPart("/huan/syn1", "/huan/syn1_part");
    worker_finalize();
    return 0;
}
