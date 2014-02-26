#include "blogel_reach_vorPart.h"

int main(int argc, char* argv[])
{
    init_workers();
    blogel_reach_vorPart("", "");
    worker_finalize();
    return 0;
}
