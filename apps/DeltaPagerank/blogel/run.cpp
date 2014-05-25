#include "blogel_app_deltapr.h"
#include "unistd.h"
int main(int argc, char* argv[])
{
    init_workers();
    blogel_app_deltapr("/url/webbase", "/exp/pr1");
    sleep(10);
    blogel_app_deltapr("/url/webuk", "/exp/pr2");
    sleep(10);
    blogel_app_deltapr("/vor/webbase", "/exp/pr3");
    sleep(10);
    blogel_app_deltapr("/vor/webuk", "/exp/pr4");
    sleep(10);
    worker_finalize();
    return 0;
}
