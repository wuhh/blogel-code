#include "utils/communication.h"
#include "blogel/BVertex.h"
#include "blogel/Block.h"
#include "blogel/BType.h"
#include "blogel/BWorker.h"
#include "blogel/BGlobal.h"
#include <queue>
#include <iostream>
using namespace std;

int src=0;//100
//int src = 44881114;
int dst=-1;//10000;

struct ReachVertexValue
{
    char tag;
    vector<triplet> in_edges;
    vector<triplet> out_edges;
    int inSplit;//v.in-edge[0, ..., inSplit] are local to block
    int outSplit;//v.out-edge[0, ..., outSplit] are local to block
};

//set field "active" if v.tag changes
//set back after block-computing
class ReachVertex:public BVertex<VertexID, ReachVertexValue, char>
{
public:
    virtual void compute(MessageContainer & messages)
    {
        if(step_num()>1)
        {
            char tag=0;
            for(MessageIter it=messages.begin(); it!=messages.end(); it++)
            {
                tag|=(*it);
            }
            char & mytag=value().tag;
            if((tag|mytag) != mytag)
            {
                mytag|=tag;
                if(mytag==3)
                    forceTerminate();
            }//remain active, to be processed by block-computing
            else
                vote_to_halt();
        }
    }
};

//====================================

class ReachBlock:public Block<char, ReachVertex, char>
{
public:
    virtual void compute(MessageContainer & messages, VertexContainer & vertexes)
    {
        //collect active seeds
        queue<int> q1;
        queue<int> q2;
        for(int i=begin; i<begin + size; i++)
        {
            ReachVertex & vertex=*(vertexes[i]);
            if(vertex.is_active())
            {
                if(vertex.value().tag==1)
                    q1.push(i);
                else if(vertex.value().tag==2)
                    q2.push(i);
                else
                {
                    forceTerminate();
                    return;
                }
            }
        }
        //forward_propagate
        while(!q1.empty())
        {
            int cur=q1.front();
            q1.pop();
            ReachVertex & v=*(vertexes[cur]);
            vector<triplet> & out_edges=v.value().out_edges;
            //in-block propagation
            for(int i=0; i<=v.value().outSplit; i++)
            {
                triplet & nb=out_edges[i];
                int idx=nb.wid;//field "worker" is now index of vertexes
                ReachVertex & nbv=*(vertexes[idx]);
                char & tag=nbv.value().tag;
                if((tag|1)!=tag)
                {
                    tag|=1;
                    if(tag==3)
                    {
                        forceTerminate();
                        return;
                    }
                    else
                        q1.push(idx);
                }
            }
            //out-block propagation
            char & tag=v.value().tag;
            for(int i=v.value().outSplit+1; i<out_edges.size(); i++)
            {
                triplet & nb=out_edges[i];
                v.send_message(nb.vid, nb.wid, tag);
            }
            v.vote_to_halt();
        }
        //backward_propagate
        while(!q2.empty())
        {
            int cur=q2.front();
            q2.pop();
            ReachVertex & v=*(vertexes[cur]);
            vector<triplet> & in_edges=v.value().in_edges;
            //in-block propagation
            for(int i=0; i<=v.value().inSplit; i++)
            {
                triplet & nb=in_edges[i];
                int idx=nb.wid;//field "worker" is now index of vertexes
                ReachVertex & nbv=*(vertexes[idx]);
                char & tag=nbv.value().tag;
                if((tag|2)!=tag)
                {
                    tag|=2;
                    if(tag==3)
                    {
                        forceTerminate();
                        return;
                    }
                    else
                        q2.push(idx);
                }
            }
            //out-block propagation
            char & tag=v.value().tag;
            for(int i=v.value().inSplit+1; i<in_edges.size(); i++)
            {
                triplet & nb=in_edges[i];
                v.send_message(nb.vid, nb.wid, tag);
            }
            v.vote_to_halt();
        }
        vote_to_halt();
    }
};

//====================================

class ReachBlockWorker:public BWorker<ReachBlock>
{
    char buf[1000];

public:
    virtual void blockInit(VertexContainer & vertexes, BlockContainer & blocks)
    {
        hash_map<int, int> map;
        for(int i=0; i<vertexes.size(); i++)
            map[vertexes[i]->id]=i;
        //////
        if(_my_rank==MASTER_RANK)
            cout<<"Splitting in/out-block edges ..."<<endl;
        for(BlockIter it=blocks.begin(); it!=blocks.end(); it++)
        {
            ReachBlock* block=*it;
            for(int i=block->begin; i<block->begin + block->size; i++)
            {
                ReachVertex* vertex=vertexes[i];
                vector<triplet> & in_edges=vertex->value().in_edges;
                vector<triplet> tmp;
                vector<triplet> tmp1;
                for(int j=0; j<in_edges.size(); j++)
                {
                    if(in_edges[j].bid==block->bid)
                    {
                        in_edges[j].wid=map[in_edges[j].vid];//workerID->array index
                        tmp.push_back(in_edges[j]);
                    }
                    else
                        tmp1.push_back(in_edges[j]);
                }
                in_edges.swap(tmp);
                vertex->value().inSplit=in_edges.size()-1;
                in_edges.insert(in_edges.end(), tmp1.begin(), tmp1.end());
                //--------------------
                vector<triplet> & out_edges=vertex->value().out_edges;
                tmp.clear();
                tmp1.clear();
                for(int j=0; j<out_edges.size(); j++)
                {
                    if(out_edges[j].bid==block->bid)
                    {
                        out_edges[j].wid=map[out_edges[j].vid];//workerID->array index
                        tmp.push_back(out_edges[j]);
                    }
                    else
                        tmp1.push_back(out_edges[j]);
                }
                out_edges.swap(tmp);
                vertex->value().outSplit=out_edges.size()-1;
                out_edges.insert(out_edges.end(), tmp1.begin(), tmp1.end());
            }
        }
        if(_my_rank==MASTER_RANK)
            cout<<"In/out-block edges split"<<endl;
    }
    virtual ReachVertex* toVertex(char* line)
    {
        char * pch;
        pch=strtok(line, " ");
        ReachVertex* v=new ReachVertex;
        v->id=atoi(pch);
        pch=strtok(NULL, " ");
        v->bid=atoi(pch);
        pch=strtok(NULL, "\t");
        v->wid=atoi(pch);
        vector<triplet> & in_edges=v->value().in_edges;
        vector<triplet> & out_edges=v->value().out_edges;
        while(pch = strtok(NULL, " "))
        {
            triplet trip;
            trip.vid=atoi(pch);
            pch=strtok(NULL, " ");
            pch=strtok(NULL, " ");
            trip.bid=atoi(pch);
            pch=strtok(NULL, " ");
            trip.wid=atoi(pch);
            in_edges.push_back(trip);
            out_edges.push_back(trip);
        }
        if(v->id==src)
            v->value().tag=1;
        else if(v->id==dst)
            v->value().tag=2;
        else
        {
            v->value().tag=0;
            v->vote_to_halt();
        }
        return v;
    }

    virtual void toline(ReachBlock* b, ReachVertex* v,BufferedWriter& writer)
    {
        if(v->value().tag==1)
        {
            char buf[1024];
            sprintf(buf, "%d\n", v->id);
            writer.write(buf);
        }
    }
};

class ReachCombiner:public Combiner<char>
{
public:
    virtual void combine(char & old, const char & new_msg)
    {
        old|=new_msg;
    }
};


void blogel_app_reach(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path=in_path;
    param.output_path=out_path;
    param.force_write=true;
    ReachBlockWorker worker;
    worker.set_compute_mode(ReachBlockWorker::VB_COMP);
    ReachCombiner combiner;
    worker.setCombiner(&combiner);
    worker.run(param);
}
