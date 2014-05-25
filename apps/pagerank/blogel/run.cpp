#include "blogel_app_pagerank1.h"
//#include "blogel_app_pagerank2.h"
int main(int argc, char* argv[])
{
    init_workers();
    blogel_app_pagerank1("/url/webbase","/exp/pr1");
  //  blogel_app_pagerank2("/exp/pr1", "/exp/pr5");
    sleep(10);

    blogel_app_pagerank1("/url/webuk","/exp/pr2");
  //  blogel_app_pagerank2("/exp/pr2", "/exp/pr5");
    sleep(10);
    
    blogel_app_pagerank1("/vor/webbase","/exp/pr3");
   // blogel_app_pagerank2("/exp/pr3", "/exp/pr5");
    sleep(10);

    blogel_app_pagerank1("/vor/webuk","/exp/pr4");
   // blogel_app_pagerank2("/exp/pr4", "/exp/pr5");
    sleep(10);



    worker_finalize();
    return 0;
}
