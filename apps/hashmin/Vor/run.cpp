#include "blogel_hashmin_vorPart.h"

int main(int argc, char* argv[])
{
    init_workers();
    blogel_hashmin_vorPart("/pullgel/webuk", "/vor/webuk");
    worker_finalize();
    return 0;
}
