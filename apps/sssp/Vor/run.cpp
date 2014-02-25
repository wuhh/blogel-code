#include "blogel_app_vorPart.h"

int main(int argc, char* argv[])
{
    init_workers();
    blogel_app_vorPart("","");
    worker_finalize();
    return 0;
}
