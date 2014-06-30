#include "utils/communication.h"
#include "blogel/BVertex.h"
#include "blogel/Block.h"
#include "blogel/BType.h"
#include "blogel/BWorker.h"
#include "blogel/BGlobal.h"

#include <iostream>
using namespace std;

struct CCVertexValue {
    vector<triplet> edges;
};
ibinstream& operator<<(ibinstream& m, const CCVertexValue& v)
{
    m << v.edges;
    return m;
}

obinstream& operator>>(obinstream& m, CCVertexValue& v)
{
    m >> v.edges;
    return m;
}

//====================================

class CCVertex : public BVertex<VertexID, CCVertexValue, VertexID> {
public:
    virtual void compute(MessageContainer& messages)
    {
    } //not used
};

//====================================

struct tuple {
    int bid;
    int wid;
};
ibinstream& operator<<(ibinstream& m, const tuple& v)
{
    m << v.bid;
    m << v.wid;
    return m;
}

obinstream& operator>>(obinstream& m, tuple& v)
{
    m >> v.bid;
    m >> v.wid;
    return m;
}
struct CCBlockValue {
    int color;
    vector<tuple> edges;
};

ibinstream& operator<<(ibinstream& m, const CCBlockValue& v)
{
    m << v.color;
    m << v.edges;
    return m;
}

obinstream& operator>>(obinstream& m, CCBlockValue& v)
{
    m >> v.color;
    m >> v.edges;
    return m;
}

//====================================

class CCBlock : public Block<CCBlockValue, CCVertex, VertexID> {
public:
    void broadcast(VertexID msg)
    {
        vector<tuple>& nbs = value().edges;
        for (int i = 0; i < nbs.size(); i++) {
            send_message(nbs[i].bid, nbs[i].wid, msg);
        }
    }

    virtual void compute(MessageContainer& messages, VertexContainer& vertexes)
    {
        if (step_num() == 1) {
            VertexID min = bid;
            vector<tuple>& nbs = value().edges;
            for (int i = 0; i < nbs.size(); i++) {
                if (min > nbs[i].bid)
                    min = nbs[i].bid;
            }
            value().color = min;
            broadcast(min);
            vote_to_halt();
        } else {
            VertexID min = messages[0];
            for (int i = 1; i < messages.size(); i++) {
                if (min > messages[i])
                    min = messages[i];
            }
            if (min < value().color) {
                value().color = min;
                broadcast(min);
            }
            vote_to_halt();
        }
    }
};

//====================================

class CCBlockWorker : public BWorker<CCBlock> {
    char buf[1000];

public:
    virtual void blockInit(VertexContainer& vertexes, BlockContainer& blocks)
    {
        if (step_num() > 1)
            return; //only for step 1
        if (_my_rank == MASTER_RANK)
            cout << "Initializing block edges ..." << endl;

        long long blockE = 0, VE = 0;

        for (BlockIter it = blocks.begin(); it != blocks.end(); it++) {
            CCBlock* block = *it;
            hash_set<int> duplicate; //avoid duplicated neighbors
            block->value().color = block->bid; //init color
            for (int i = block->begin; i < block->begin + block->size; i++) {
                CCVertex* vertex = vertexes[i];
                vector<triplet>& vedges = vertex->value().edges;
                for (int j = 0; j < vedges.size(); j++) {
                    triplet& cur = vedges[j];
                    hash_set<int>::iterator sit = duplicate.find(cur.bid);
                    VE += 1;
                    if (sit == duplicate.end()) {
                        duplicate.insert(cur.bid);
                        tuple nb;
                        nb.bid = cur.bid;
                        nb.wid = cur.wid;
                        block->value().edges.push_back(nb); //collect different neighbors
                    }
                }
            }
            blockE += block->value().edges.size();
        }
        long long global_blockE = master_sum_LL(blockE);
        long long global_VE = master_sum_LL(VE);
        if (_my_rank == MASTER_RANK) {
            cout << "Block edges initialized" << endl;
            cout << "Vertex edges (2x): " << global_VE << endl;
            cout << "Block edges (2x): " << global_blockE << endl;
        }
    }

    //C version
    virtual CCVertex* toVertex(char* line)
    {
        char* pch;
        pch = strtok(line, " ");
        CCVertex* v = new CCVertex;
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        v->bid = atoi(pch);
        pch = strtok(NULL, "\t");
        v->wid = atoi(pch);
        //color will be assigned in Superstep 1
        
       // pch = strtok(NULL, " ");
       // int num = atoi(pch);
        while (pch = strtok(NULL," ")) {
            triplet cur;
         //   pch = strtok(NULL, " ");
            cur.vid = atoi(pch);
           // pch = strtok(NULL, " ");
            pch = strtok(NULL, " ");
            cur.bid = atoi(pch);
            pch = strtok(NULL, " ");
            cur.wid = atoi(pch);
            v->value().edges.push_back(cur);
        }
        return v;
    }

    virtual void toline(CCBlock* b, CCVertex* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d\t%d", v->id, b->value().color);
        writer.write(buf);
        writer.write("\n");
    }
};

class CCBlockCombiner : public Combiner<VertexID> {
public:
    virtual void combine(VertexID& old, const VertexID& new_msg)
    {
        if (old > new_msg)
            old = new_msg;
    }
};

//todo: block-level combiner

void blogel_app_hashmin(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    CCBlockWorker worker;
    CCBlockCombiner combiner;
    worker.setBCombiner(&combiner);
    worker.run(param);
}
