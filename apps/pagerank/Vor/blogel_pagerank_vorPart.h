#include "blogel/Voronoi.h"
#include "blogel/BAssign.h"
#include <iostream>
#include <sstream>
#include "blogel/BGlobal.h"
using namespace std;

class MyWorker : public BAssignWorker {
public:
    //vid \t bid numIn in1 in2 ... numOut out1 out2
    virtual BAssignVertex* toVertex(char* line)
    {
        char* pch;
        BAssignVertex* v = new BAssignVertex;
        pch = strtok(line, "\t");
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        v->value().block = atoi(pch);
        pch = strtok(NULL, " ");
        int num = atoi(pch);

        for (int i = 0; i < num; i++) {
            pch = strtok(NULL, " ");
            int nb = atoi(pch);
            v->value().neighbors.push_back(nb);
        }
        return v;
    }

    //me \t nb1 nb2 ...
    //each item is of format "vertexID blockID workerID"
    virtual void toline(BAssignVertex* v, BufferedWriter& writer) //key: "vertexID blockID slaveID"
    { //val: list of "vid block slave "
        char buf[1024];
        sprintf(buf, "%d %d %d\t", v->id, v->value().block, _my_rank);
        writer.write(buf);

        vector<triplet>& vec = v->value().nbsInfo;
        for (int i = 0; i < vec.size(); i++) {
            sprintf(buf, "%d %d %d ", vec[i].vid, vec[i].bid, vec[i].wid);
            writer.write(buf);
        }
        writer.write("\n");
    }
};

void blogel_pagerank_vorPart(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    //////
    MyWorker worker;
    worker.run(param);
}
