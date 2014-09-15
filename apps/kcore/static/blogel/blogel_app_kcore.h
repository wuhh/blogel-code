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
    int degree;
    bool changed;
    virtual void compute(MessageContainer& messages)
    {
        changed = false; // changed = false  at the beginning of each superstep

        vector<triplet>& in_edges = value().in_edges;
        vector<triplet>& out_edges = value().out_edges;
         
        if(step_num() == 1)
        {
            degree = in_edges.size() + out_edges.size();
            phi = degree;
            for(int i = 0 ;i < out_edges.size(); i ++)
            {
                intpair msg(id, degree);
                send_message(out_edges[i].vid, out_edges[i].wid, msg);
            }
        }
        else if(step_num() == 2)
        {
            for(int i = 0; i < messages.size(); i ++)
            {
                psi[ messages[i].v1 ] = messages[i].v2;
            }
        }
        else
        {
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

class kcoreBlock : public Block<char, kcoreVertex, intpair> {
public:
    vector< pair<int, vector<int>* > > Bplus;

    void binsort(VertexContainer& vertexes)
    {
        sort(Bplus.begin(), Bplus.end(), cmpPsi);
        /*
        cout << "DEBUG: Bplus" << endl;

        for(int i = 0; i < Bplus.size(); i ++)
        {
            cout << "##### " <<  Bplus[i].first << endl;
            vector<int>* adj = Bplus[i].second;
            for(int k = 0; k < adj->size(); k ++)
            {
                cout <<  vertexes[(*adj)[k]]->id << " ";
            }
            cout << endl;
        }
        */

        int maxDeg = 0;
        for(int i = begin; i < begin + size; i ++)
        {
            kcoreVertex* v = vertexes[i];
            maxDeg = max(maxDeg, v->degree);  
        }
       
        std::vector< std::list<int>::iterator > pos;
        std::vector< std::list<int> > bin; // for binsort
        
        pos.resize(size, std::list<int>::iterator()); // size is the size of block // i - begin to get the index
        bin.resize(maxDeg + 1);
        
        for(int i = begin; i < begin + size; i ++)
        {

            kcoreVertex* v = vertexes[i];
            //cout << "add vid: " << v->id << " to bin: " << v->degree << endl;
            //assert(v->degree < bin.size());
            //assert(i - begin < pos.size());
            bin[ v->degree ].push_back(i); // i is the index, if you want to get the vertex, use vertexes[i]
            pos[ i - begin ] = --bin[ v->degree ].end();
        }
        /*
        cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << endl;
        for(int kk = 0; kk < bin.size(); kk ++)
        {
            cout << "DEBUG: bin " << kk << endl;
            for(list<int>::iterator it = bin[kk].begin(); it != bin[kk].end() ; it ++)
            {
                cout <<  *it << " "; 
            }
            cout << endl;
        }
        cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << endl;
        */
        int i = 0, j = 0;
        int d_min, psi_min = inf;
        // update d_min
        while(i < maxDeg && bin[i].empty()) i ++;
        if(i == maxDeg) return;
        else d_min = i;
        // update psh_min
        if(j < Bplus.size()) psi_min = psi[Bplus[j].first];

        int S = size;

        while( S > 0 ) // while |S| > 0
        {
            //cout << "bid = " << bid << " psi_min = " << psi_min << " d_min = " << d_min << endl;
            while(psi_min < d_min)
            {
                //cout << "!!!! id: " << Bplus[j].first << endl;
                vector<int>* adj = Bplus[j].second;
                /*
                for(int k = 0; k < adj->size(); k ++)
                {
                       cout <<  vertexes[(*adj)[k]]->id << " ";
                }
                cout << endl << "!!!!!!!!!" << endl;
                */
                for(int k = 0; k < adj->size(); k ++)
                {
                    int idx = (*adj)[k];
                    kcoreVertex* v = vertexes[idx];

                    // move the vertex to another bin
                    //assert(idx - begin < pos.size());
                    list<int>::iterator pt = pos[idx - begin];

                    //move to another bin
                    //cout << " id: " << v->id << " decrease to " << v->degree - 1 << endl;
                    //assert(v->degree < bin.size());
                    bin[ v->degree  ].erase(pt);
                    v->degree --;
                    //assert(v->degree >= 0);
                    // erase
                    bin[ v->degree ].push_back(idx);
                    //assert(idx - begin < pos.size());
                    pos[ idx - begin ] = --bin[ v->degree ].end();
                }
                // update d_min
                i --; //???????
                while(i < maxDeg && bin[i].empty()) i ++;
                if(i == maxDeg) return;
                else d_min = i;
                // update psh_min
                j ++;
                if(j < Bplus.size())  psi_min = psi[Bplus[j].first];
                else psi_min = inf;
            }
            int idx = bin[i].front();
            kcoreVertex* v = vertexes[idx];
            //cout << "id: " << v->id << " degree: " << v->degree << " phi: " << v->phi << endl;
            if (v->degree < v->phi)
            {
                v->phi = v->degree;
                v->changed = true;
            }

            for(int k = 0; k < v->value().in_edges.size(); k ++)
            {
                int uidx = v->value().in_edges[k].wid; 
                kcoreVertex* u = vertexes[uidx];

                if(u->degree > v->degree)
                {
                    // move the vertex to another bin
                    //assert(uidx - begin < pos.size());
                    list<int>::iterator pt = pos[uidx - begin];
                    //move to another bin
                    if( u->degree > i)
                    {
                        //assert(u->degree < bin.size());
                        bin[ u->degree  ].erase(pt);
                        u->degree --;
                        // erase
                        //assert(u->degree < bin.size());
                        bin[ u->degree ].push_back(uidx);
                        //assert(uidx - begin < pos.size());
                        pos[ uidx - begin ] = --bin[ u->degree ].end();
                    }
                }
            }
            //remove v
            list<int>::iterator pt = pos[idx - begin];
            
            //cout << "before bin size: " << bin[v->degree].size() << " to remove vid: " << *pt << endl;
            /*
            for(int kk = 0; kk < bin.size(); kk ++)
            {
                cout << "DEBUG: bin " << kk << endl;
                for(list<int>::iterator it = bin[kk].begin(); it != bin[kk].end() ; it ++)
                {
                    cout <<  *it << " "; 
                }
                cout << endl;
            }
            */
            bin[ v->degree  ].erase(pt);
            //cout << "remove " << v->id << " from bin " << v->degree << " , bin size = " << bin[v->degree].size() << endl;
            S --;
            // update d_min
            while(i < maxDeg && bin[i].empty()) i ++;
            if(i == maxDeg) return;
            else d_min = i;
        }
    }
    virtual void compute(MessageContainer& messages, VertexContainer& vertexes)
    {
        if(step_num() == 1)
        {
            // initialize Bplus

            hash_map<int, vector<int>* > extend;

            for(int i = begin; i < begin + size; i ++)
            {
                kcoreVertex* v = vertexes[i];
                vector<triplet>& out_edges = v->value().out_edges;
                for(int j = 0; j < out_edges.size(); j ++)
                {
                    int vid = out_edges[j].vid;

                    if(extend.count(vid) == 0)
                    {
                        extend[vid] = new vector<int>();
                    }
                    extend[vid]->push_back(i); // i is the subscript
                }
            }

            for(hash_map<int, vector<int>* >::iterator it = extend.begin(); it != extend.end(); it ++)
            {
                /*
                cout << "~~~~~~~: " << it->first << endl;
                vector<int> vec = *(it->second);
                for(int i = 0 ;i < vec.size(); i ++)
                {
                    cout << vertexes[ vec[i]  ]->id << " ";
                }
                cout << endl;
                */
                Bplus.push_back(*it);
            }
        }
        else if(step_num() > 1)
        {
            // call algo5 binsort
            binsort(vertexes);

            // send msgs
            for(int i = begin; i < begin + size; i ++)
            {
                kcoreVertex* v = vertexes[i];
                if(v->changed)
                {
                    vector<triplet>& out_edges = v->value().out_edges;
                    for(int j = 0; j < out_edges.size(); j ++)
                    {
                        intpair msg(v->id, v->phi);
                        v->send_message(out_edges[i].vid, out_edges[i].wid, msg);
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
