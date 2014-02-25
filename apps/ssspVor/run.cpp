#include "ssspPart.h"

int main(int argc, char* argv[]){
	init_workers();
	run_ssspPart();
	worker_finalize();
	return 0;
}


