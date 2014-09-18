#include "utils/communication.h"
#include "utils/Combiner.h"
#include "blogel/BVertex.h"
#include "blogel/Block.h"
#include "blogel/BWorker.h"
#include "blogel/BGlobal.h"
#include "blogel/BType.h"
#include <list>
#include <iostream>
#include <vector>
#include <cassert>
#include <sstream>
#include <queue>
using namespace std;
const int inf = 1e9;
hash_map<int,int> psi;

struct kcoreValue {
    vector<triplet> in_edges;
    vector<triplet> out_edges;
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
    int phi;
    hash_map<int, int> P;
    bool changed;
    virtual void compute(MessageContainer& messages)
    {
        vector<triplet>& in_edges = value().in_edges;
        vector<triplet>& out_edges = value().out_edges;
        changed = true;
        if(step_num() == 1)
        {
            phi = in_edges.size() + out_edges.size();
            for(int i = 0 ; i < out_edges.size(); i ++)
            {
                send_message(out_edges[i].vid, out_edges[i].wid, intpair(id, phi));
                P[out_edges[i].vid] = inf;
            }
            if(phi == 0)
            {
                changed = false;
            }
        }
        else
        {
            for(int i = 0 ; i < messages.size(); i ++)
            {
                int u = messages[i].v1;
                int k = messages[i].v2;
                if(P[u] > k)
                    P[u] = k;
            }
        }
        vote_to_halt();
    }
};

class kcoreBlock : public Block<char, kcoreVertex, intpair> {
public:
    int subfunc(kcoreVertex* v, VertexContainer& vertexes)
    {
        vector<triplet>& in_edges = v->value().in_edges;
        vector<triplet>& out_edges = v->value().out_edges;

        vector<int> cd(v->phi + 2, 0);

        for(int i = 0 ; i < in_edges.size(); i ++)
        {
            kcoreVertex* u = vertexes[ in_edges[i].wid ];
            if(v->phi < u->phi)
            {
                u->phi = v->phi;
                u->activate();
            }
            cd[ u->phi ] ++;
        }
        for(int i = 0 ;i < out_edges.size(); i ++)
        {
            if(v->phi < v->P[ out_edges[i].vid ])
            {
                v->P[ out_edges[i].vid ] = v->phi;
            }
            cd[ v->P[ out_edges[i].vid ] ] ++ ;
        }
        for(int i = v->phi; i >= 1; i --)
        {
            cd[i] += cd[i + 1];
            if(cd[i] >= i)
                return i;
        }
        assert(0);

    }
    virtual void compute(MessageContainer& messages, VertexContainer& vertexes)
    {
        for(int i = begin; i < begin + size; i ++)
        {
            kcoreVertex* v = vertexes[i];
            vector<triplet>& in_edges = v->value().in_edges;
            vector<triplet>& out_edges = v->value().out_edges;

            if(v->changed)
            {
                v->changed = false;
                int x = subfunc(v, vertexes);
                if(x < v->phi)
                {
                    v->phi = x;
                    for(int j = 0 ; j < out_edges.size(); j ++)
                    {
                        if(v->phi < v->P[ out_edges[j].vid  ])
                        {
                            send_message(out_edges[j].vid, out_edges[j].wid, intpair(v->id, v->phi));
                        }
                    }
                }
            }
        }
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
                vector<triplet>& out_edges = vertex->value().out_edges;

                for (int j = 0; j < in_edges.size(); j++) {
                    in_edges[j].wid = map[in_edges[j].vid]; //workerID->array index
                }

                for (int j = 0; j < out_edges.size(); j++) {
                    psi[ out_edges[j].vid ] = inf;
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
        vector<triplet>& out_edges = v->value().out_edges;
        int num;
        ssin >> num;
        for (int i = 0; i < num; i++) {
            int vid, bid, wid;
            ssin >> vid >> bid >> wid;
            triplet trip;
            trip.vid = vid;
            trip.bid = bid;
            trip.wid = wid;
            if(bid == v->bid)
            {
                in_edges.push_back(trip);
            }
            else
            {
                out_edges.push_back(trip);
            }
        }
        return v;
    }

    virtual void toline(kcoreBlock* b, kcoreVertex* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d\t%d\n", v->id, v->phi);
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
