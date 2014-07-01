//#include "blogel_app_pagerank1.h"
#include "blogel_app_pagerank2.h"
int main(int argc, char* argv[])
{
    init_workers();

    //    blogel_app_pagerank1("/vor/webbase","/exp/pr3");
    blogel_app_pagerank2("/exp/pr3", "/exp/pr5");

    worker_finalize();
    return 0;
}
