//app: PageRank

#include "blogel/STRPart.h"
#include <iostream>
#include <sstream>
#include "utils/global.h"
using namespace std;

//input line format: id x y \t nb1 nb2 ...

class ssspSTRRnd1:public STRWorker
{
	char buf[1000];
	public:

		ssspSTRRnd1(int xnum, int ynum, double sampleRate):STRWorker(xnum, ynum, sampleRate){}

		//C version
		virtual STRVertex* toVertex(char* line)
		{
			char * pch;
			STRVertex* v=new STRVertex;
			v->content=line;//first set content!!! line will change later due to "strtok"
			pch=strtok(line, " ");
			v->id=atoi(pch);
			pch=strtok(NULL, " ");
			v->x=atof(pch);
			pch=strtok(NULL, "\t");
			v->y=atof(pch);
			//////
			while(pch=strtok(NULL, " "))
			{
				int nb=atoi(pch);
				v->neighbors.push_back(nb);
				strtok(NULL, " ");//edge length
			}
			return v;
		}

		virtual char* toline(STRVertex* v)//key: "vertexID blockID workerD"
		{//val: list of "vid bid wid"
			sprintf(buf, "%d %d %d\t", v->id, v->bid, _my_rank);
			int len=strlen(buf);
			char tmp[50];//just for length calculation
			vector<triplet> & vec=v->nbsInfo;
			hash_map<int, triplet> map;
			for(int i=0; i<vec.size(); i++){
				map[vec[i].vid]=vec[i];
			}
			////////
			stringstream ss(v->content);
			string token;
			ss>>token;//vid
			ss>>token;//x
			ss>>token;//y
			while(ss>>token)
			{
				int vid=atoi(token.c_str());
				ss>>token;
				double elen=atof(token.c_str());
				triplet trip=map[vid];
				sprintf(tmp, "%d %f %d %d ", vid, elen, trip.bid, trip.wid);
				strcat(buf+len, tmp);
				len+=strlen(tmp);
			}
			return buf;
		}
};

void run_strpart1()
{
	int xnum=4;
	int ynum=4;
	double sampleRate=0.05;
	//////
	WorkerParams param;
	param.input_path="/OL-coord";
	param.output_path="/OL_STR_parted";
	param.force_write=true;
	param.native_dispatcher=false;
	ssspSTRRnd1 worker(xnum, ynum, sampleRate);
	worker.run(param);
}
