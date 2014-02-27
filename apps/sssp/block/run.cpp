#include "blogel_app_sssp.h"
int main(int argc, char* argv[])
{
    init_workers();
    if(argv[1][0] == 'e')
    {
        blogel_app_sssp("/str/euroxy_2", "/exp/sssp");
    }
    else
    {
        blogel_app_sssp("/str/usaxy_2", "/exp/sssp");
    }
    worker_finalize();
    return 0;
}
