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
#include <list>
using namespace std;
const int inf = 1000000000;
hash_map<int,int> psi;

int CURRENT_PI = 0;

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

struct kcoretValue
{
    vector<tripletX> in_edges;
    vector<tripletX> out_edges;
};

ibinstream& operator<<(ibinstream& m, const kcoretValue& v)
{
    m << v.in_edges;
    m << v.out_edges;
    return m;
}

obinstream& operator>>(obinstream& m, kcoretValue& v)
{
    m >> v.in_edges;
    m >> v.out_edges;
    return m;
}

class kcoretVertex : public BVertex<VertexID, kcoretValue, intpair>
{
public:
    vector<intpair> phis; // K, T
    hash_map<int, int> p;
    
    int phi;
    int degree;
    bool changed;
    bool deleted;
    
    void add_phi()
    {
        if (phis.size() == 0 || CURRENT_PI < phis.back().v2)
            phis.push_back(intpair(CURRENT_PI, phi)); // num_out_edges should equal to num_in_edges
        else
            phis.back().v1 = CURRENT_PI;
    }
    
    virtual void compute(MessageContainer& messages)
    {
        vector<tripletX>& in_edges = value().in_edges;
        vector<tripletX>& out_edges = value().out_edges;
        
        if(step_num() == 1)
        {
            // add last round result
            degree = in_edges.size() + out_edges.size();
            
            if(phase_num() > 1 && degree > 0)
                add_phi();
            // clear edges
            while(in_edges.size() && in_edges.back().vid.v2 == CURRENT_PI)
                in_edges.pop_back();
            while(out_edges.size() && out_edges.back().vid.v2 == CURRENT_PI)
                out_edges.pop_back();
            // for agg
                
            degree = in_edges.size() + out_edges.size();
            phi = degree;
            
            if(phase_num() == 1) // initialize once
            {
                for(int i = 0 ;i < out_edges.size(); i ++)
                {
                    //intpair msg(id, degree);
                    //send_message(out_edges[i].vid, out_edges[i].wid, msg);
                    psi[ out_edges[i].vid.v1 ] = inf;
                }
            }

            changed = false;
            
            if(degree == 0)
            {
                vote_to_halt();
            }
        }
        else
        {   
           
            if(step_num() == 2)
            {
                CURRENT_PI = *((int*)getAgg());
            }
            
            changed = true;
            
            // update psi based on messages received.
            for(int i = 0; i < messages.size(); i ++)
            {
                int u = messages[i].v1;
                int k = messages[i].v2;
                if (k < psi[u])
                {
                    psi[u] = k;
                }
            }

            vote_to_halt();
        }
    }
};

int cmpPsi(const pair<int, vector<int>* >& p1, const pair<int, vector<int>* >& p2) 
{
    return psi[p1.first] < psi[p2.first];
}

int cmptripletX(const tripletX& t1, const tripletX& t2) 
{
    return t1.vid.v2 > t2.vid.v2;
}


class kcoretBlock : public Block<char, kcoretVertex, char>
{
public:
    vector< pair<int, vector<int>* > > Bplus;

        void binsort(VertexContainer& vertexes)
        {
            if(step_num() > 1)
                sort(Bplus.begin(), Bplus.end(), cmpPsi);

            int maxDeg = 0;
            for(int i = begin; i < begin + size; i ++)
            {
                kcoretVertex* v = vertexes[i];
                maxDeg = max(maxDeg, v->degree);  
            }

            std::vector< std::list<int>::iterator > pos;
            std::vector< std::list<int> > bin; // for binsort

            pos.resize(size, std::list<int>::iterator()); // size is the size of block // i - begin to get the index
            bin.resize(maxDeg + 1);

            for(int i = begin; i < begin + size; i ++)
            {

                kcoretVertex* v = vertexes[i];
                bin[ v->degree ].push_back(i); // i is the index, if you want to get the vertex, use vertexes[i]
                pos[ i - begin ] = --bin[ v->degree ].end();
            }
            int i = 0, j = 0;
            int d_min, psi_min = inf;
            // update d_min
            while(i <= maxDeg && bin[i].empty()) i ++;
            if(i > maxDeg) return;
            else d_min = i;
            // update psh_min
            if(j < Bplus.size()) 
            {
                psi_min = psi[Bplus[j].first];
            }

            int S = size;
            while( S > 0 ) // while |S| > 0
            {
                while(psi_min < d_min)
                {
                    vector<int>* adj = Bplus[j].second;
                    for(int k = 0; k < adj->size(); k ++)
                    {
                        int idx = (*adj)[k];
                        kcoretVertex* v = vertexes[idx];
                        if(v->deleted) continue;
                        // move the vertex to another bin
                        list<int>::iterator pt = pos[idx - begin];

                        //move to another bin
                        bin[ v->degree  ].erase(pt);
                        v->degree --;
                        // erase
                        bin[ v->degree ].push_back(idx);
                        pos[ idx - begin ] = --bin[ v->degree ].end();
                    }
                    // update d_min
                    i --; //???????
                    while(i <= maxDeg && bin[i].empty()) i ++;
                    if(i > maxDeg) return;
                    else d_min = i;
                    // update psh_min
                    j ++;
                    if(j < Bplus.size())  psi_min = psi[Bplus[j].first];
                    else psi_min = inf;
                }
                int idx = bin[i].front();
                kcoretVertex* v = vertexes[idx];
                if (v->degree < v->phi)
                {
                    v->phi = v->degree;
                    v->changed = true;
                }

                for(int k = 0; k < v->value().in_edges.size(); k ++)
                {
                    int uidx = v->value().in_edges[k].wid; 
                    kcoretVertex* u = vertexes[uidx];
                    if(u->deleted) continue;
                    if(u->degree > v->degree)
                    {
                        // move the vertex to another bin
                        list<int>::iterator pt = pos[uidx - begin];
                        //move to another bin

                        bin[ u->degree  ].erase(pt);
                        u->degree --;
                        // erase
                        bin[ u->degree ].push_back(uidx);
                        pos[ uidx - begin ] = --bin[ u->degree ].end();
                    }
                }
                //remove v
                list<int>::iterator pt = pos[idx - begin];
                v->deleted = true;


                bin[ v->degree  ].erase(pt);
                S --;
                // update d_min
                while(i <= maxDeg && bin[i].empty())
                {
                    i ++;
                }
                if(i > maxDeg) return;
                else d_min = i;
            }
        }
        int subfunc(kcoretVertex* v, VertexContainer& vertexes)
        {
            //cout << "subfunc" << endl;
            vector<int> cd(v->phi + 2 , 0);
            vector<tripletX>& in_edges = v->value().in_edges;
            vector<tripletX>& out_edges = v->value().out_edges;
            //cout << "###";
            for(int i = 0; i < in_edges.size(); i ++)
            {
                kcoretVertex* u = vertexes[ in_edges[i].wid ];
                cd[  min(v->phi, u->phi) ] ++;

                //cout << u->id << " " << u->phi << "#";
            }
            //cout << endl;
            //cout << "!!!!";
            for(int i = 0; i < out_edges.size(); i ++)
            {
                //cout <<  out_edges[i].vid << " " <<   psi[ out_edges[i].vid ]  << "#";
                
                // ????????????????
                /*
                if( psi[ out_edges[i].vid ] > v->phi )
                {
                    psi[ out_edges[i].vid ] = v->phi;
                }
                */
                cd[ min(psi[ out_edges[i].vid.v1], v->phi) ] ++;
            }
            //cout << endl;

            for(int i = v->phi; i >= 1 ; i --)
            {
                cd[i] += cd[i + 1];
                if(cd[i] >= i)
                {
                    return i;
                }
            }
            assert(0);
        } 
        virtual void compute(MessageContainer& messages, VertexContainer& vertexes)
        {
            if(step_num() == 1)
            {
                // initialize Bplus

                hash_map<int, vector<int>* > extend;

                for(int i = begin; i < begin + size; i ++)
                {
                    kcoretVertex* v = vertexes[i];
                    vector<tripletX>& out_edges = v->value().out_edges;
                    for(int j = 0; j < out_edges.size(); j ++)
                    {
                        int vid = out_edges[j].vid.v1;

                        if(extend.count(vid) == 0)
                        {
                            extend[vid] = new vector<int>();
                        }
                        extend[vid]->push_back(i); // i is the subscript
                    }
                }

                for(hash_map<int, vector<int>* >::iterator it = extend.begin(); it != extend.end(); it ++)
                {
                    Bplus.push_back(*it);
                }
                // call algo5 binsort
                for(int i = begin; i < begin + size; i ++)
                {
                    kcoretVertex* v = vertexes[i];
                    vector<tripletX>& in_edges = v->value().in_edges;
                    vector<tripletX>& out_edges = v->value().out_edges;
                    v->changed = false;
                    v->deleted = false;
                    v->degree = in_edges.size() + out_edges.size();
                }

                binsort(vertexes);
                // send msgs
                for(int i = begin; i < begin + size; i ++)
                {
                    kcoretVertex* v = vertexes[i];
                    //if(v->changed)
                    {
                        vector<tripletX>& out_edges = v->value().out_edges;
                        for(int j = 0; j < out_edges.size(); j ++)
                        {
                            if(v->phi < psi[out_edges[j].vid.v1])
                            {
                                intpair msg(v->id, v->phi);
                                v->send_message(out_edges[j].vid.v1, out_edges[j].wid, msg);
                            }
                        }
                    }
                }
            }
            else
            {
                for(int i = begin; i < begin + size; i ++)
                {
                    kcoretVertex* v = vertexes[i];
                    //cout << v->id << " " << v->phi << endl;
                    
                    if(v->changed)
                    {
                        v->changed = false;
                        vector<tripletX>& in_edges = v->value().in_edges;
                        vector<tripletX>& out_edges = v->value().out_edges;
                        int x = subfunc(v, vertexes);
                        //cout << "~~~~" << v->id << " " << v->phi <<" " << x <<  endl;
                        if(x < v->phi)
                        {
                            v->phi = x;
                            for(int j = 0; j < in_edges.size(); j ++)
                            {
                                kcoretVertex* u = vertexes[ in_edges[j].wid ];
                                u->activate(); //??????????
                            }
                            for(int j = 0; j < out_edges.size(); j ++)
                            {
                                if(v->phi < psi[ out_edges[j].vid.v1 ] )
                                {
                                    v->send_message(out_edges[j].vid.v1, out_edges[j].wid, intpair(v->id, v->phi));
                                }
                            }
                        }
                    }
                }
            }
            vote_to_halt();
        }
};

class kcoretAgg : public BAggregator<kcoretVertex, kcoretBlock, int, int>
{
    int pi;
public:
    virtual void init()
    {
        pi = inf;
    }

    virtual void stepPartialV(kcoretVertex* v)
    {
        if(step_num() == 1)
        {
            if(v->value().in_edges.size() != 0)
                pi = min(pi, v->value().in_edges.back().vid.v2);
            if(v->value().out_edges.size() != 0)
                pi = min(pi, v->value().out_edges.back().vid.v2);
        }
    }

    virtual void stepPartialB(kcoretBlock* b)
    {
        ; //not used
    }

    virtual void stepFinal(int* part)
    {
        pi = min(pi, *part);
    }

    virtual int* finishPartial()
    {
        return &pi;
    }

    virtual int* finishFinal()
    {
        if(step_num() == 1)
        {
            cout << "@@@@@@@@@@@@@@ " << pi << endl;
            if(pi == inf)
            {
                forceTerminate();
            }
        }
        return &pi;
    }
};

//====================================

class kcoretBlockWorker : public BWorker<kcoretBlock, kcoretAgg>
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
        for (BlockIter it = blocks.begin(); it != blocks.end(); it++) {
            kcoretBlock* block = *it;
            for (int i = block->begin; i < block->begin + block->size; i++) {
                kcoretVertex* vertex = vertexes[i];
                vector<tripletX>& in_edges = vertex->value().in_edges;
                vector<tripletX>& out_edges = vertex->value().out_edges;

                for (int j = 0; j < in_edges.size(); j++) {
                    in_edges[j].wid = map[in_edges[j].vid.v1]; //workerID->array index
                }

                for (int j = 0; j < out_edges.size(); j++) {
                    psi[ out_edges[j].vid.v1 ] = inf;
                }
            }
        }
        if (_my_rank == MASTER_RANK) {
            cout << "In/out-block edges split" << endl;
        }
    }

    virtual kcoretVertex* toVertex(char* line)
    {

        kcoretVertex* v = new kcoretVertex;
        istringstream ssin(line);
        ssin >> v->id;
        ssin >> v->bid;
        ssin >> v->wid;

        vector<tripletX>& in_edges = v->value().in_edges;
        vector<tripletX>& out_edges = v->value().out_edges;

        int num;
        ssin >> num;
        for (int i = 0; i < num; i++)
        {
            tripletX trip;
            ssin >> trip.vid.v1 >> trip.bid >> trip.wid >> trip.vid.v2;
            for (int j = 0; j < trip.vid.v2; j++)
            {
                int t;
                ssin >> t;
            }

            if(trip.bid == v->bid)
            {
                in_edges.push_back(trip);
            }
            else
            {
                out_edges.push_back(trip);
            }
        }

        sort(in_edges.begin(),in_edges.end(),cmptripletX);
        sort(out_edges.begin(),out_edges.end(),cmptripletX);
        return v;
    }

    virtual void toline(kcoretBlock* b, kcoretVertex* v, BufferedWriter& writer)
    {
        //add the result from last round
        sprintf(buf, "%d\t", v->id);
        writer.write(buf);
        for (int i = 0; i < v->phis.size(); i++)
        {
            if (i != 0)
            {
                sprintf(buf, " ");
                writer.write(buf);
            }
            sprintf(buf, "%d %d", v->phis[i].v1, v->phis[i].v2);
            writer.write(buf);
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
    kcoretBlockWorker worker;
    worker.set_compute_mode(kcoretBlockWorker::VB_COMP);
    kcoretAgg agg;
    worker.setAggregator(&agg);
    worker.run(param, inf);
}
