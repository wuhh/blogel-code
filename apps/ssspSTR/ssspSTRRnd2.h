#include "blogel/STRPartR2.h"
#include <iostream>
#include <sstream>
#include "utils/global.h"
using namespace std;

class ssspSTRRnd2: public STR2Worker
{
	char buf[1000];
	public:

		//C version
		virtual STR2Vertex* toVertex(char* line)
		{
			STR2Vertex* v=new STR2Vertex;
			v->value().content=line;//first set content!!! line will change later due to "strtok"
			char * pch;
			pch=strtok(line, " ");
			v->id=atoi(pch);
			pch=strtok(NULL, " ");
			v->bid=atoi(pch);
			pch=strtok(NULL, "\t");
			v->wid=atoi(pch);
			vector<triplet> & edges=v->value().neighbors;
			while(pch=strtok(NULL, " "))
			{
				triplet trip;
				trip.vid=atoi(pch);
				pch=strtok(NULL, " ");//length
				pch=strtok(NULL, " ");
				trip.bid=atof(pch);
				pch=strtok(NULL, " ");
				trip.wid=atoi(pch);
				edges.push_back(trip);
			}
			return v;
		}

		virtual char* toline(STR2Block* b, STR2Vertex* v)
		{
			sprintf(buf, "%d %d %d\t", v->id, v->value().new_bid, _my_rank);
			int len=strlen(buf);
			char tmp[50];//just for length calculation
			vector<triplet> & vec=v->value().neighbors;
			hash_map<int, triplet> map;
			for(int i=0; i<vec.size(); i++){
				map[vec[i].vid]=vec[i];
			}
			////////
			stringstream ss(v->value().content);
			string token;
			ss>>token;//vid
			ss>>token;//myBlock
			ss>>token;//myWorker
			while(ss>>token)
			{
				int vid=atoi(token.c_str());
				ss>>token;
				double elen=atof(token.c_str());
				ss>>token;//filter out old blockID
				ss>>token;//filter out workerID
				triplet trip=map[vid];
				sprintf(tmp, "%d %f %d %d ", vid, elen, trip.bid, trip.wid);
				strcat(buf+len, tmp);
				len+=strlen(tmp);
			}
			return buf;
		}
};

int run_strpart2(){
	WorkerParams param;
	param.input_path="/OL_STR_parted";
	param.output_path="/OL_STR_rnd2";
	param.force_write=true;
	ssspSTRRnd2 worker;
	worker.run(param);
}
