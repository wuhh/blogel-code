#include "blogel_hashmin_vorPart.h"

int main(int argc, char* argv[])
{
    init_workers();
    blogel_hashmin_vorPart("/random/25m", "/vor/25m");
    blogel_hashmin_vorPart("/random/50m", "/vor/50m");
    blogel_hashmin_vorPart("/random/75m", "/vor/75m");
    blogel_hashmin_vorPart("/random/100m", "/vor/100m");
    worker_finalize();
    return 0;
}
