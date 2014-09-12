#include "utils/communication.h"
#include "utils/Combiner.h"
#include "blogel/BVertex.h"
#include "blogel/Block.h"
#include "blogel/BWorker.h"
#include "blogel/BGlobal.h"
#include "blogel/BType.h"
#include <iostream>
#include <vector>
#include <cassert>
#include <sstream>
#include <queue>
using namespace std;

const int inf = 1000000000;

struct tripletX {
    VertexID vid;
    int bid;
    int wid;

    int degree;
    
    bool operator==(const triplet& o) const
    {
        return vid == o.vid;
    }

    friend ibinstream& operator<<(ibinstream& m, const tripletX& idm)
    {
        m << idm.vid;
        m << idm.bid;
        m << idm.wid;
        m << idm.degree;
        return m;
    }
    friend obinstream& operator>>(obinstream& m, tripletX& idm)
    {
        m >> idm.vid;
        m >> idm.bid;
        m >> idm.wid;
        m >> idm.degree;
        return m;
    }
};

struct kcoreValue {
    vector<triplet> in_edges;
    vector<tripletX> out_edges;
};

ibinstream& operator<<(ibinstream& m, const kcoreValue& v)
{
    m << v.in_edges;
    m << v.out_edges;
    return m;
}

obinstream& operator>>(obinstream& m, kcoreValue& v)
{
    m >> v.in_edges;
    m >> v.out_edges;
    return m;
}

class kcoreVertex : public BVertex<VertexID, kcoreValue, intpair> {
public:
    int K;
    virtual void compute(MessageContainer& messages)
    {
        vector<triplet>& in_edges = value().in_edges;
        vector<tripletX>& out_edges = value().out_edges;
        int degree = in_edges.size() + out_edges.size();
        if(step_num() == 1)
        {
            cout << id << endl;
            for(int i = 0 ;i < out_edges.size(); i ++)
            {
                intpair msg(id, degree);
                send_message(out_edges[i].vid, out_edges[i].wid, msg);
            }
            vote_to_halt();
        }
        else if(step_num() == 2)
        {
            hash_map<int,int> deg;
            for(int i = 0; i < messages.size(); i ++)
            {
                deg[ messages[i].v1 ] = messages[i].v2;
            }
            for(int i = 0 ; i <  out_edges.size(); i ++)
            {
                assert(deg.count(out_edges[i].vid));
                out_edges[i].degree = deg[ out_edges[i].vid  ];
            }
            vote_to_halt();
        }
    }
};

class kcoreBlock : public Block<char, kcoreVertex, intpair> {
public:
    virtual void compute(MessageContainer& messages, VertexContainer& vertexes)
    {

        // call algo6

        // send msgs
        vote_to_halt();
    }
};

//====================================

class kcoreBlockWorker : public BWorker<kcoreBlock> {
    char buf[1000];

public:
    virtual void blockInit(VertexContainer& vertexes, BlockContainer& blocks)
    {
        hash_map<int, int> map;
        for (int i = 0; i < vertexes.size(); i++)
            map[vertexes[i]->id] = i;
        //////
        if (_my_rank == MASTER_RANK)
            cout << "Splitting in/out-block edges ..." << endl;
        for (BlockIter it = blocks.begin(); it != blocks.end(); it++) {
            kcoreBlock* block = *it;
            for (int i = block->begin; i < block->begin + block->size; i++) {
                kcoreVertex* vertex = vertexes[i];
                vector<triplet>& in_edges = vertex->value().in_edges;
        
                for (int j = 0; j < in_edges.size(); j++) {
                    if (in_edges[j].bid == block->bid) {
                        in_edges[j].wid = map[in_edges[j].vid]; //workerID->array index
                    }
                }
            }
        }
        if (_my_rank == MASTER_RANK) {
            cout << "In/out-block edges split" << endl;
        }
    }

    virtual kcoreVertex* toVertex(char* line)
    {
        kcoreVertex* v = new kcoreVertex;
        istringstream ssin(line);
        ssin >> v->id;
        ssin >> v->bid;
        ssin >> v->wid;
        vector<triplet>& in_edges = v->value().in_edges;
        vector<tripletX>& out_edges = v->value().out_edges;
        int num;
        ssin >> num;
        for (int i = 0; i < num; i++) {
            int vid, bid, wid;
            ssin >> vid >> bid >> wid;
            if(bid == v->bid)
            {
                triplet trip;
                trip.vid = vid;
                trip.bid = bid;
                trip.wid = wid;
                in_edges.push_back(trip);
            }
            else
            {
                tripletX trip;
                trip.vid = vid;
                trip.bid = bid;
                trip.wid = wid;
                trip.degree = -1;
                out_edges.push_back(trip);
            }
        }
        return v;
    }

    virtual void toline(kcoreBlock* b, kcoreVertex* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d\t%d\n", v->id, v->K);
        writer.write(buf);
    }
};

void blogel_app_kcore(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    kcoreBlockWorker worker;
    worker.set_compute_mode(kcoreBlockWorker::VB_COMP);
    worker.run(param);
}
