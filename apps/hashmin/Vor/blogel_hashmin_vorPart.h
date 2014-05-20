#include "blogel/Voronoi.h"
#include <iostream>
#include <sstream>
#include "blogel/BGlobal.h"
using namespace std;

class MyWorker : public BPartWorker {
public:
    char buf[1024];
    //C version
    virtual BPartVertex* toVertex(char* line)
    {
        char* pch;
        pch = strtok(line, "\t");
        BPartVertex* v = new BPartVertex;
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        int num = atoi(pch);
        //v->value().color=-1;//default is -1
        while (num--) {
            pch = strtok(NULL, " ");
            v->value().neighbors.push_back(atoi(pch));
        }
        return v;
    }

    virtual void toline(BPartVertex* v, BufferedWriter& writer) //key: "vertexID blockID slaveID"
    { //val: list of "vid block slave "
        sprintf(buf, "%d %d %d\t", v->id, v->value().color, _my_rank);
        writer.write(buf);
        vector<triplet>& vec = v->value().nbsInfo;
        for (int i = 0; i < vec.size(); i++) {
            sprintf(buf, "%d %d %d ", vec[i].vid, vec[i].bid, vec[i].wid);
            writer.write(buf);
        }
        writer.write("\n");
    }
};

void blogel_hashmin_vorPart(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    bool to_undirected = false;
    //livej friend
    /*
    set_sampRate(0.001);
    set_maxHop(10);
    set_maxVCSize(100000);
    set_factor(2.0);
    set_stopRatio(0.9);
    set_maxRate(0.1);
    */
    //btc
    /*
    set_sampRate(0.001);
    set_maxHop(20);
    set_maxVCSize(500000);
    set_factor(2.0);
    set_stopRatio(0.95);
    set_maxRate(0.1);
    */
    //webuk
    /*
    set_sampRate(0.001);
    set_maxHop(30);
    set_maxVCSize(500000);
    set_factor(1.6);
    set_stopRatio(1.0);
    set_maxRate(0.2);
*/
    //usa
    /*
    set_sampRate(0.001);
    set_maxHop(50);
    //set_maxVCSize(500000);
    set_factor(2.0);
    set_stopRatio(0.9);
    set_maxRate(0.1);
    */

    //random
    set_sampRate(0.001);
    set_maxHop(20);
    set_maxVCSize(500000);
    set_factor(2.0);
    set_stopRatio(0.9);
    set_maxRate(0.1);


    MyWorker worker;
    worker.run(param, to_undirected);
}
