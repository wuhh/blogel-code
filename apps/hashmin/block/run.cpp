#include "blogel_app_hashmin.h"

int main(int argc, char* argv[])
{
    init_workers();
    if(argv[1][0] == 'e')
    {
        blogel_app_hashmin("/str/euroxy_2", "/exp/cc");
    }
    else
    {
        blogel_app_hashmin("/str/usaxy_2", "/exp/cc");
    }
    worker_finalize();
    return 0;
}
