#include "utils/communication.h"
#include "blogel/BVertex.h"
#include "blogel/Block.h"
#include "blogel/BType.h"
#include "blogel/BWorker.h"
#include "blogel/BGlobal.h"

#include <iostream>
#include "math.h"
using namespace std;

#define EPS 0.01

struct PRValue {
    double pr;
    double delta;
    vector<triplet> edges;
};

//====================================

struct doublepair {
    bool converge;
    double accum; //pr sum for nodes with out-degree 0
};
ibinstream& operator<<(ibinstream& m, const doublepair& v)
{
    m << v.converge;
    m << v.accum;
    return m;
}

obinstream& operator>>(obinstream& m, doublepair& v)
{
    m >> v.converge;
    m >> v.accum;
    return m;
}

//====================================

class PRVertex : public BVertex<VertexID, PRValue, double> {
public:
    virtual void compute(MessageContainer& messages)
    {
        if (step_num() == 1) {
            //value().pr is initialized during data loading
            value().delta = EPS + 1; //init delta>EPS
        } else {
            if (step_num() == 2) {
                doublepair* agg = ((doublepair*)getAgg());
                value().pr /= agg->accum;
                //value().pr = 1.0 / get_vnum();
            } else {
                double sum = 0;
                for (MessageIter it = messages.begin(); it != messages.end(); it++) {
                    sum += *it;
                }
                doublepair* agg = ((doublepair*)getAgg());
                if (agg->converge) {
                    vote_to_halt();
                    return;
                }
                double residual = agg->accum / get_vnum();
                double newVal = 0.15 / get_vnum() + 0.85 * (sum + residual);
                value().delta = fabs(newVal - value().pr);
                value().pr = newVal;
            }
            for (vector<triplet>::iterator it = value().edges.begin(); it != value().edges.end(); it++) {
                send_message(it->vid, it->wid, value().pr / value().edges.size());
            }
        }
    }
};

//====================================

class PRBlock : public Block<char, PRVertex, char> //msg = weight * pr
                {
public:
    virtual void compute(MessageContainer& messages, VertexContainer& vertexes)
    {
    } //not used
};

//====================================

class PRSum : public BAggregator<PRVertex, PRBlock, doublepair, doublepair> {
private:
    doublepair pair;

public:
    virtual void init()
    {
        pair.converge = true;
        pair.accum = 0;
    }

    virtual void stepPartialV(PRVertex* v)
    {
        if (step_num() == 1) {
            pair.accum += v->value().pr;
        } else {
            if (v->value().edges.size() == 0)
                pair.accum += v->value().pr;
            if (v->value().delta > EPS / get_vnum())
                pair.converge = false;
        }
    }

    virtual void stepPartialB(PRBlock* b) {}; //not used

    virtual void stepFinal(doublepair* part)
    {
        pair.accum += part->accum;
        if (part->converge == false)
            pair.converge = false;
    }

    virtual doublepair* finishPartial()
    {
        return &pair;
    }

    virtual doublepair* finishFinal()
    {
        return &pair;
    }
};

//====================================

class PRWorker : public BWorker<PRBlock, PRSum> {
    char buf[1024];
    int initMode;

public:
    enum COMPUTEMODE {
        PAPER_MODE = 0,
        LOCALPR_BLOCKSIZE = 1,
        LOCALPR_BLOCKSIZE_BLOCKPR = 2,
        BLOCKPR = 3
    };

    PRWorker(int mode)
    {
        initMode = mode;
    }

    virtual void blockInit(VertexContainer& vertexes, BlockContainer& blocks)
    {
    } //nothing to do

    //input line format: me \t v.local-pr block(v).pr |block(v)| nb1 nb2 ...
    //each item is of format "vertexID blockID workerID"
    virtual PRVertex* toVertex(char* line)
    {
        char* pch;
        PRVertex* v = new PRVertex;
        pch = strtok(line, " ");
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        v->bid = atoi(pch);
        pch = strtok(NULL, "\t");
        v->wid = atoi(pch);
        pch = strtok(NULL, " ");
        double vpr = atof(pch);
        pch = strtok(NULL, " ");
        double bpr = atof(pch);
        pch = strtok(NULL, " ");
        int bsize = atoi(pch);
        if (initMode == PAPER_MODE)
            v->value().pr = vpr * bpr;
        else if (initMode == LOCALPR_BLOCKSIZE)
            v->value().pr = vpr * bsize;
        else if (initMode == LOCALPR_BLOCKSIZE_BLOCKPR)
            v->value().pr = vpr * bsize * bpr;
        else
            v->value().pr = bpr;
        while (pch = strtok(NULL, " ")) {
            triplet trip;
            trip.vid = atoi(pch);
            pch = strtok(NULL, " ");
            trip.bid = atoi(pch);
            pch = strtok(NULL, " ");
            trip.wid = atoi(pch);
            v->value().edges.push_back(trip);
        }
        return v;
    }

    virtual void toline(PRBlock* b, PRVertex* v, BufferedWriter& writer)
    {

        sprintf(buf, "%d \t %e\n", v->id, v->value().pr);
        writer.write(buf);
    }
};

class PRCombiner : public Combiner<double> {
public:
    virtual void combine(double& old, const double& new_msg)
    {
        old += new_msg;
    }
};

void blogel_app_pagerank2(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    enum COMPUTEMODE {
        PAPER_MODE = 0,
        LOCALPR_BLOCKSIZE = 1,
        LOCALPR_BLOCKSIZE_BLOCKPR = 2,
        BLOCKPR = 3
    };
    PRWorker worker(0);
    worker.set_compute_mode(PRWorker::V_COMP); //important!!! criterion for ending
    PRCombiner combiner;
    worker.setCombiner(&combiner);
    PRSum agg;
    worker.setAggregator(&agg);
    worker.run(param);
}
