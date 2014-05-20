#include "utils/communication.h"
#include "utils/Combiner.h"
#include "blogel/BVertex.h"
#include "blogel/Block.h"
#include "blogel/BWorker.h"
#include "blogel/BGlobal.h"
#include "blogel/BType.h"
#include <iostream>
#include <vector>

//#define ROUND 10

using namespace std;

struct DeltaValue
{
    double pr;
    double delta;
    vector<triplet> edges;
    int split; //v.edges[0, ..., inSplit] are local to block
};

ibinstream& operator<<(ibinstream& m, const DeltaValue& v)
{
    m << v.pr;
    m << v.delta;
    m << v.edges;
    return m;
}

obinstream& operator>>(obinstream& m, DeltaValue& v)
{
    m >> v.pr;
    m >> v.delta;
    m >> v.edges;
    return m;
}

//====================================

class DeltaVertex : public BVertex<VertexID, DeltaValue, double>
{
public:
    virtual void compute(MessageContainer& messages)
    {
        if (step_num() == 1)
        {
            value().pr = 0;
            value().delta = 0.15;
        }
        else
        {
            for (int i = 0; i < messages.size(); i++) value().delta += messages[i];
        }
    }
};

//====================================

class DeltaBlock : public Block<char, DeltaVertex, char>
{
public:
    
	virtual void compute(MessageContainer& messages, VertexContainer& vertexes) //multi-source Dijkstra (assume a super src node)
    {
//		if(step_num() > ROUND) vote_to_halt();
		
        for (int i = begin; i < begin + size; i++)
        {
			DeltaVertex& vertex = *(vertexes[i]);
			if(step_num() > ROUND) vertex.vote_to_halt();
			else
			{
				if(vertex.value().delta>0)
				{
                    int split = vertex.value().split;
                    vector<triplet>& edges = vertex.value().edges;
					vertex.value().pr+=vertex.value().delta;
					double update=0.85*vertex.value().delta/edges.size();
					//in-block processing
					for (int i = 0; i <= split; i++)
					{
						triplet& v = edges[i];
						DeltaVertex& nb = *(vertexes[v.wid]);
						nb.value().delta+=update;
					}
					//out-block msg passing
					for (int i = split + 1; i < edges.size(); i++)
					{
						triplet& v = edges[i];
						send_message(v.vid, v.wid, update);
					}
				}
				vertex.value().delta=0;
			}
        }
    }
};

//====================================

class DeltaBlockWorker : public BWorker<DeltaBlock>
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
            DeltaBlock* block = *it;
            for (int i = block->begin; i < block->begin + block->size; i++)
            {
                DeltaVertex* vertex = vertexes[i];
                vector<triplet>& edges = vertex->value().edges;
                vector<triplet> tmp;
                vector<triplet> tmp1;
                for (int j = 0; j < edges.size(); j++)
                {
                    if (edges[j].bid == block->bid)
                    {
                        edges[j].wid = map[edges[j].vid]; //workerID->array index
                        tmp.push_back(edges[j]);
                    }
                    else
                        tmp1.push_back(edges[j]);
                }
                edges.swap(tmp);
                vertex->value().split = edges.size() - 1;
                edges.insert(edges.end(), tmp1.begin(), tmp1.end());
            }
        }
        if (_my_rank == MASTER_RANK)
        {
            cout << "In/out-block edges split" << endl;
        }
    }

    virtual DeltaVertex* toVertex(char* line)
    {
        char* pch;
        pch = strtok(line, " ");
        DeltaVertex* v = new DeltaVertex;
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        v->bid = atoi(pch);
        pch = strtok(NULL, "\t");
        v->wid = atoi(pch);
        vector<triplet>& edges = v->value().edges;
        pch = strtok(NULL, " ");
        int num = atoi(pch);

        while (num --)
        {
            triplet trip;
            pch = strtok(NULL, " ");
            trip.vid = atoi(pch);
            pch = strtok(NULL, " ");
            trip.bid = atoi(pch);
            pch = strtok(NULL, " ");
            trip.wid = atoi(pch);
            edges.push_back(trip);
        }
        return v;
    }

    virtual void toline(DeltaBlock* b, DeltaVertex* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d\t%f\n", v->id, v->value().pr);
        writer.write(buf);
    }
    //@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
};

class DeltaCombiner : public Combiner<double>
{
public:
    virtual void combine(double& old, const double& new_msg)
    {
        old = + new_msg;
    }
};

void blogel_app_deltapr(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    DeltaBlockWorker worker;
    worker.set_compute_mode(DeltaBlockWorker::VB_COMP);
    DeltaCombiner combiner;
    worker.setCombiner(&combiner);
    worker.run(param);
}
