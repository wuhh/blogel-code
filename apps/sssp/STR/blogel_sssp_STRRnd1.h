//app: PageRank

#include "blogel/STRPart.h"
#include <iostream>
#include <sstream>
#include "blogel/BGlobal.h"
using namespace std;

//input line format: id x y \t nb1 nb2 ...

class STRRnd1 : public STRWorker {
    char buf[1000];

public:
    STRRnd1(int xnum, int ynum, double sampleRate)
        : STRWorker(xnum, ynum, sampleRate)
    {
    }

    //C version
    virtual STRVertex* toVertex(char* line)
    {
        char* pch;
        STRVertex* v = new STRVertex;
        v->content = line; //first set content!!! line will change later due to "strtok"
        pch = strtok(line, " ");
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        v->x = atof(pch);
        pch = strtok(NULL, "\t");
        v->y = atof(pch);
        pch = strtok(NULL, " ");
        int num = atoi(pch);
        while (num--) {
            pch = strtok(NULL, " ");
            int nb = atoi(pch);
            v->neighbors.push_back(nb);
            strtok(NULL, " "); //edge length
        }
        return v;
    }

    virtual void toline(STRVertex* v, BufferedWriter& writer) //key: "vertexID blockID workerD"
    { //val: list of "vid bid wid"
        sprintf(buf, "%d %d %d\t", v->id, v->bid, _my_rank);
        writer.write(buf);

        int len = strlen(buf);
        vector<triplet>& vec = v->nbsInfo;
        hash_map<int, triplet> map;
        for (int i = 0; i < vec.size(); i++) {
            map[vec[i].vid] = vec[i];
        }
        ////////
        stringstream ss(v->content);
        string token;
        ss >> token; //vid
        ss >> token; //x
        ss >> token; //y
        ss >> token; //num
        while (ss >> token) {
            int vid = atoi(token.c_str());
            ss >> token;
            double elen = atof(token.c_str());
            triplet trip = map[vid];
            sprintf(buf, "%d %f %d %d ", vid, elen, trip.bid, trip.wid);
            writer.write(buf);
        }
        writer.write("\n");
    }
};

void blogel_sssp_STRRnd1(string in_path, string out_path)
{
    int xnum = 20;
    int ynum = 20;
    double sampleRate = 0.01;
    //////
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    STRRnd1 worker(xnum, ynum, sampleRate);
    worker.run(param);
}
