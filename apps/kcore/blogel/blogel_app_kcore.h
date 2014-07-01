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

using namespace std;

const int inf = 1000000000;


struct tripletX
{
    intpair vid;
    int bid;
    int wid;

    bool operator==(const tripletX& o) const
    {
        return vid == o.vid;
    }

    friend ibinstream& operator<<(ibinstream& m, const tripletX& idm)
    {
        m << idm.vid;
        m << idm.bid;
        m << idm.wid;
        return m;
    }
    friend obinstream& operator>>(obinstream& m, tripletX& idm)
    {
        m >> idm.vid;
        m >> idm.bid;
        m >> idm.wid;
        return m;
    }
};

struct kcoreT2Value
{
    vector<intpair> K; // K, T
    vector<tripletX> edges; // vid, no of edges
    hash_map<int,int> p;
    int split; //v.edges[0, ..., inSplit] are local to block
    vector<intpair> buffer;
};


ibinstream& operator<<(ibinstream& m, const kcoreT2Value& v)
{
    m << v.K;
    m << v.edges;
    m << v.p;
    m << v.split;
    return m;
}

obinstream& operator>>(obinstream& m, kcoreT2Value& v)
{
    m >> v.K;
    m >> v.edges;
    m >> v.p;
    m >> v.split;
    return m;
}

//====================================

class kcoreT2Vertex : public BVertex<VertexID, kcoreT2Value, intpair>
{
public:
    virtual void compute(MessageContainer& messages)
    {
        if (step_num() > 3)
        {
            vector<intpair>& Kvec = value().K;
            vector<tripletX>& edges = value().edges;
            hash_map<int,int>& p = value().p;


            // To be consistent with edges list;
            for (int i = 0; i < messages.size(); i++)
            {
                int v = messages[i].v1;

                if (messages[i].v2 < p[v])
                    p[v] = messages[i].v2;
            }
            vote_to_halt();
        }
    }
};

class kcoreT2Block : public Block<char, kcoreT2Vertex, char>
{
public:
    int currentT;

    int subfunc(kcoreT2Vertex* v)
    {
        vector<tripletX>& edges = v->value().edges;
        hash_map<int,int>& p = v->value().p;
        int K = v->value().K.back().v1;
        vector<int> cd(K + 2, 0);
        for (int i = 0; i < edges.size(); i++)
        {
            int v = edges[i].vid.v1;
            if (p[v] > K)
                p[v] = K;
            cd[p[v]]++;
        }
        for (int i = K; i >= 1; i--)
        {
            cd[i] += cd[i + 1];
            if (cd[i] >= i)
                return i;
        }
        assert(0);
    }
    virtual void compute(MessageContainer& messages, VertexContainer& vertexes)
    {

        for (int i = begin; i < begin + size; i++)
        {
            kcoreT2Vertex& vertex = *(vertexes[i]);
            vector<intpair>& Kvec = vertex.value().K;
            vector<tripletX>& edges = vertex.value().edges;
            hash_map<int,int>& p = vertex.value().p;
            int split = vertex.value().split;

            if (step_num() == 1)
            {
                if (Kvec.size() >= 2)
                {
                    int lastone = Kvec.size() - 1, lasttwo = Kvec.size() - 2;
                    if (Kvec[lastone].v1 == Kvec[lasttwo].v1)
                    {
                        swap(Kvec[lastone], Kvec[lasttwo]);
                        Kvec.pop_back();
                    }
                }

                return;
            }
            else if (step_num() == 2)
            {
                currentT = *((int*)getAgg());
                if(phase_num()>1 &&false)
                {
                    vertex.value().split = -1;

                    vector<tripletX> newedges;
                    for(int i = 0 ;i < edges.size();i ++)
                    {
                        if(edges[i].vid.v2 > currentT)
                        {
                            newedges.push_back(edges[i]);
                            if(edges[i].bid == bid)
                                vertex.value().split  ++;
                        }

                    }
                    edges.swap(newedges);
                }
                return;
            }
            else if (step_num() == 3)
            {
                currentT = *((int*)getAgg());

            }

            if(step_num() == 3)
            {

                if (Kvec.size() == 0)
                    Kvec.push_back(intpair(edges.size(), currentT));
                else
                    Kvec.push_back(intpair(min((int)edges.size(), Kvec.back().v1), currentT));
                int K = Kvec.back().v1;

                p.clear();

                for(int i = 0 ;i < edges.size(); i ++)
                {
                    p[edges[i].vid.v1] = inf;
                }


                for (int j = 0; j <= split; j++)
                {

                    tripletX& v = edges[j];

                    kcoreT2Vertex& nb = *(vertexes[v.wid]);
                    if(K < nb.value().p[vertex.id])
                    {
                        nb.value().p[vertex.id] = K;
                    }
                }

                //out-block msg passing
                for (int j = split + 1;j < edges.size(); j++)
                {
                    tripletX& v = edges[j];
                    vertex.send_message(v.vid.v1, v.wid, intpair(vertex.id, K));
                }
            }
            else
            {

                int K = Kvec.back().v1;
                int x = subfunc(&vertex); //

                if (x < K)
                {
                    K = x;

                    for (int j = 0; j <= split; j++)
                    {
                        if (K < p[j])
                        {
                            tripletX& v = edges[j];
                            kcoreT2Vertex& nb = *(vertexes[v.wid]);
                            if(K < nb.value().p[vertex.id])
                            {
                                nb.value().p[vertex.id] = K;
                            }
                        }
                    }
                    //out-block msg passing
                    for (int j = split + 1; j < edges.size(); j++)
                    {
                        if (K < p[j])
                        {
                            tripletX& v = edges[j];
                            vertex.send_message(v.vid.v1, v.wid, intpair(vertex.id, K));
                        }

                    }
                    Kvec.back().v1 = K;
                }

            }
        }
        vote_to_halt();
    }
};

class kcoreT2Agg : public BAggregator<kcoreT2Vertex, kcoreT2Block, int, int>
{
private:
    int currentT;

public:
    virtual void init()
    {
        currentT = inf;
    }

    virtual void stepPartialV(kcoreT2Vertex* v)
    {
        if (v->value().edges.size() != 0)
        {
            currentT = min(currentT, v->value().edges.back().vid.v2);
        }
    }


    virtual void stepPartialB(kcoreT2Block* b)
    {
        ; //not used
    }

    virtual void stepFinal(int* part)
    {
        currentT = min(currentT, *part);
    }

    virtual int* finishPartial()
    {
        return &currentT;
    }

    virtual int* finishFinal()
    {
        if (currentT == inf)
            forceTerminate();
        return &currentT;
    }
};


//====================================

class kcoreT2BlockWorker : public BWorker<kcoreT2Block,kcoreT2Agg>
{
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
        for (BlockIter it = blocks.begin(); it != blocks.end(); it++)
        {
            kcoreT2Block* block = *it;
            for (int i = block->begin; i < block->begin + block->size; i++)
            {
                kcoreT2Vertex* vertex = vertexes[i];
                vector<tripletX>& edges = vertex->value().edges;
                vector<tripletX> tmp;
                vector<tripletX> tmp1;
                for (int j = 0; j < edges.size(); j++)
                {
                    if (edges[j].bid == block->bid)
                    {
                        edges[j].wid = map[edges[j].vid.v1]; //workerID->array index
                        tmp.push_back(edges[j]);
                    }
                    else
                        tmp1.push_back(edges[j]);
                }
                edges.swap(tmp);

                vertex->value().split = (int)(edges.size()) - 1;

                edges.insert(edges.end(), tmp1.begin(), tmp1.end());

            }
        }
        if (_my_rank == MASTER_RANK)
        {
            cout << "In/out-block edges split" << endl;
        }
    }

    virtual kcoreT2Vertex* toVertex(char* line)
    {

        kcoreT2Vertex* v = new kcoreT2Vertex;
        istringstream ssin(line);
        ssin >> v->id;
        ssin >> v->bid;
        ssin >> v->wid;
        vector<tripletX>& edges = v->value().edges;
        int num;
        ssin >> num;
        for (int i = 0; i < num; i++)
        {
            tripletX trip;
            ssin >> trip.vid.v1 >> trip.bid >> trip.wid >> trip.vid.v2;
            for(int j = 0 ;j < trip.vid.v2 ; j ++)
            {
                int t;
                ssin >> t;
            }
            edges.push_back(trip);
        }
        return v;
    }

    virtual void toline(kcoreT2Block* b, kcoreT2Vertex* v, BufferedWriter& writer)
    {
        vector<intpair>& Kvec = v->value().K;
        if (Kvec.size() >= 2)
        {
            int lastone = Kvec.size() - 1, lasttwo = Kvec.size() - 2;
            if (Kvec[lastone].v1 == Kvec[lasttwo].v1)
            {
                swap(Kvec[lastone], Kvec[lasttwo]);
                Kvec.pop_back();
            }
        }
        sprintf(buf, "%d", v->id);
        writer.write(buf);
        for (int i = 0; i < v->value().K.size(); i++)
        {
            if (v->value().K[i].v1 != 0)
            {
                sprintf(buf, "%s%d %d", i == 0 ? "\t" : " ", v->value().K[i].v1, v->value().K[i].v2);
                writer.write(buf);
            }
        }
        writer.write("\n");
    }

};



void blogel_app_kcore(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    kcoreT2BlockWorker worker;
    worker.set_compute_mode(kcoreT2BlockWorker::VB_COMP);
    kcoreT2Agg agg;
    worker.setAggregator(&agg);
    worker.run(param);
}

