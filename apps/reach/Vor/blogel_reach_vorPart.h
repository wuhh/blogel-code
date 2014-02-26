#include "blogel/Voronoi.h"
#include <iostream>
#include <sstream>
#include "blogel/BGlobal.h"
using namespace std;

class MyWorker : public BPartWorker {
    char buf[1024];

public:
    //C version
    virtual BPartVertex* toVertex(char* line)
    {
        char* pch;
        BPartVertex* v = new BPartVertex;
        v->value().content = line; //first set content!!! line will change later due to "strtok"
        pch = strtok(line, "\t");
        v->id = atoi(pch);
        //v->value().color=-1;//default is -1
        pch = strtok(NULL, " ");
        int inNum = atoi(pch);
        std::set<int> nbs;
        for (int i = 0; i < inNum; i++) {
            pch = strtok(NULL, " ");
            nbs.insert(atoi(pch));
        }
        pch = strtok(NULL, " ");
        int outNum = atoi(pch);

        for (int i = 0; i < outNum; i++) {
            pch = strtok(NULL, " ");
            nbs.insert(atoi(pch));
        }
        for (std::set<int>::iterator it = nbs.begin(); it != nbs.end(); it++) {
            v->value().neighbors.push_back(*it);
        }
        return v;
    }

    virtual void toline(BPartVertex* v, BufferedWriter& writer) //key: "vertexID blockID slaveID"
    { //val: list of "vid block slave "
        sprintf(buf, "%d %d %d\t", v->id, v->value().color, _my_rank);
        writer.write(buf);

        vector<triplet>& vec = v->value().nbsInfo;
        hash_map<int, triplet> map;
        for (int i = 0; i < vec.size(); i++) {
            map[vec[i].vid] = vec[i];
        }
        ////////
        stringstream ss(v->value().content);

        string token;
        ss >> token; //vid
        int inNum;
        ss >> inNum;

        //sprintf(buf,"\t%d",inNum);
        //writer->write(buf);
        for (int i = 0; i < inNum; i++) {
            ss >> token;
            //int vid=atoi(token.c_str());
            //triplet trip=map[vid];
            //sprintf(buf, " %d %d %d", vid, trip.block, trip.worker);
            //writer->write(buf);
        }
        int outNum;
        ss >> outNum;
        //sprintf(buf, " %d", outNum);
        //writer->write(buf);

        for (int i = 0; i < outNum; i++) {
            ss >> token;
            int vid = atoi(token.c_str());
            triplet trip = map[vid];
            sprintf(buf, "%d %d %d ", vid, trip.bid, trip.wid);
            writer.write(buf);
        }
        writer.write("\n");
    }
};

void blogel_reach_vorPart(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    bool to_undirected = false;
    //////
    set_sampRate(0.001);
    set_maxHop(30);
    set_maxVCSize(500000);
    set_stopRatio(1.0);
    set_maxRate(0.2);
    set_factor(1.6);
    //////
    MyWorker worker;
    worker.run(param, to_undirected);
}
