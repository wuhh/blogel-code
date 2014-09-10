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

struct kcoreValue {
    int K;
    vector<triplet> edges; // vid, no of edges
    int split; //v.edges[0, ..., inSplit] are local to block
};

ibinstream& operator<<(ibinstream& m, const kcoreValue& v)
{
    m << v.K;
    m << v.edges;
    m << v.split;
    return m;
}

obinstream& operator>>(obinstream& m, kcoreValue& v)
{
    m >> v.K;
    m >> v.edges;
    m >> v.split;
    return m;
}

class kcoreVertex : public BVertex<VertexID, kcoreValue, char> {
public:
    bool changed;
    virtual void compute(MessageContainer& messages)
    {
        changed = false;
    }
};

class kcoreBlock : public Block<char, kcoreVertex, intpair> {
public:
    hash_map<int, vector<int> > VsiNeighbor;
    hash_map<int, int> VsiP;
    hash_map<int, intpair> VsiInfo; // bid, wid;
    virtual void compute(MessageContainer& messages, VertexContainer& vertexes)
    {
        if (step_num() > 1) {
            for (int i = 0; i < messages.size(); i++) {
                int k = messages[i].v2, v = messages[i].v1;
                assert(VsiP.count(v) > 0); /////
                VsiP[v] = min(VsiP[v], k);
            }
        }

        // call algo6

        // send msgs

        for (hash_map<int, vector<int> >::iterator it = VsiNeighbor.begin(); it != VsiNeighbor.end(); it++) {
            assert(VsiP.count(it->first) > 0); ////
            int pu = VsiP[it->first];
            vector<int>& nb = it->second;
            for (int i = 0; i < nb.size(); i++) {
                kcoreVertex& vertex = *vertexes[nb[i]];
                if (vertex.changed && vertex.value().K < pu) {
                    intpair pair = VsiInfo[it->first];
                    int bid = pair.v1, wid = pair.v2;
                    send_message(bid, wid, intpair(vertex.id, vertex.value().K));
                }
            }
        }
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
                vector<triplet>& edges = vertex->value().edges;
                vector<triplet> tmp;
                vector<triplet> tmp1;
                for (int j = 0; j < edges.size(); j++) {
                    if (edges[j].bid == block->bid) {
                        edges[j].wid = map[edges[j].vid]; //workerID->array index
                        tmp.push_back(edges[j]);
                    } else
                        tmp1.push_back(edges[j]);
                    //////// Initialization for neighbor set and P
                    block->VsiNeighbor[edges[j].vid].push_back(i); //array index
                    block->VsiP[edges[j].vid] = inf;
                    if (block->VsiInfo.count(edges[j].vid) == 0) {
                        block->VsiInfo[edges[j].vid] = intpair(edges[j].bid, edges[j].wid);
                    }
                }
                edges.swap(tmp);
                vertex->value().split = (int)(edges.size()) - 1;
                edges.insert(edges.end(), tmp1.begin(), tmp1.end());
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
        vector<triplet>& edges = v->value().edges;
        int num;
        ssin >> num;
        for (int i = 0; i < num; i++) {
            triplet trip;
            ssin >> trip.vid >> trip.bid >> trip.wid;
            edges.push_back(trip);
        }
        return v;
    }

    virtual void toline(kcoreBlock* b, kcoreVertex* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d\t%d\n", v->id, v->value().K);
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
    worker.run(param, inf);
}
