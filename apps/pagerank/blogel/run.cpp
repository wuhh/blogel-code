//#include "blogel_app_pagerank1.h"
#include "blogel_app_pagerank2.h"
int main(int argc, char* argv[])
{
    init_workers();
    //blogel_app_pagerank1("/vor/webuk","/exp/pr1");

    blogel_app_pagerank2("/exp/pr1", "/exp/pr2");
    worker_finalize();
    return 0;
}
