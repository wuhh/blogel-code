#include "ssspSTRRnd1.h"
#include "ssspSTRRnd2.h"

int main(int argc, char* argv[])
{
    init_workers();
    run_strpart1();
    run_strpart2();
    worker_finalize();
    return 0;
}
