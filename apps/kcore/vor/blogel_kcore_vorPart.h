#include "blogel/Voronoi.h"
#include <iostream>
#include <sstream>
#include "blogel/BGlobal.h"
#include <sstream>
using namespace std;

class vorPart : public BPartWorker
{
    char buf[1000];

public:

    //========================== for version with coordinates
    //C version
    virtual BPartVertex* toVertex(char* line)
    {
    	BPartVertex* v = new BPartVertex;
        istringstream ssin(line);
        ssin >> v->id;
        v->value().content.push_back(v->id);
        int num;
        ssin >> num;
        v->value().content.push_back(num);
        for (int i = 0; i < num; i++)
        {
            int nb, m;
            ssin >> nb >> m;
            v->value().content.push_back(nb);
            v->value().content.push_back(m);
            for (int j = 0; j < m; j++)
            {
                int t;
                ssin >> t;
                v->value().content.push_back(t);
            }
            v->value().neighbors.push_back(nb);
        }
        return v;
    }

    virtual void toline(BPartVertex* v, BufferedWriter& writer) //key: "vertexID blockID slaveID"
    { //val: list of "vid block slave "
        sprintf(buf, "%d %d %d\t", v->id, v->value().color, _my_rank);
        writer.write(buf);

        vector<triplet>& vec = v->value().nbsInfo;
        hash_map<int, triplet> map;
        for (int i = 0; i < vec.size(); i++)
        {
            map[vec[i].vid] = vec[i];
        }
        ////////
        int idx = 2;
        int num = v->value().neighbors.size();
        sprintf(buf, "%d", num);
        writer.write(buf);
        for (int i = 0; i < num; i++)
        {
            int vid = v->value().content[idx++];
            int m = v->value().content[idx++];
            triplet trip = map[vid];
            sprintf(buf," %d %d %d %d",vid,trip.bid, trip.wid, m);
            writer.write(buf);
            for(int j = 0 ; j < m;j ++)
            {
            	sprintf(buf, " %d", v->value().content[idx++]);
            	writer.write(buf);
            }

        }
        writer.write("\n");
    }
    //========================== for version with coordinates
};

int blogel_kcore_vorPart(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    bool to_undirected = false;
    //////
    //set_sampRate(0.01);
    //set_maxHop(10);
    //set_maxVCSize(100000);
    //set_factor(2);
    //set_stopRatio(0.9);
    //set_maxRate(0.1);
    //////
    vorPart worker;
    worker.run(param, to_undirected);
}
