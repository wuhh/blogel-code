//#include "blogel_app_pagerank1.h"
#include "blogel_app_pagerank2.h"
int main(int argc, char* argv[])
{
    init_workers();
    //blogel_app_pagerank1("","");
    blogel_app_pagerank2("", "");
    worker_finalize();
    return 0;
}
