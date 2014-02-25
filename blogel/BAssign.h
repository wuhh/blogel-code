#ifndef BASSIGN_H_
#define BASSIGN_H_

#include "utils/serialization.h"
#include "utils/communication.h"
#include "utils/ydhdfs.h"
#include "utils/type.h"
#include "basic/Vertex.h"
#include "basic/Worker.h"
#include "BGlobal.h"
#include <vector>
#include <iostream>
#include <stdlib.h>
#include <ext/hash_map>

struct BAssignValue
{
    int block;
    vector<VertexID> neighbors;
    vector<triplet> nbsInfo;
    string content;
};

//no vertex-add, will not be called
ibinstream & operator<<(ibinstream & m, const BAssignValue & v)
{
    m<<v.block;
    m<<v.neighbors;
    m<<v.nbsInfo;
    m<<v.content;
    return m;
}

//no vertex-add, will not be called
obinstream & operator>>(obinstream & m, BAssignValue & v)
{
    m>>v.block;
    m>>v.neighbors;
    m>>v.nbsInfo;
    m>>v.content;
    return m;
}

class BAssignVertex:public Vertex<VertexID, BAssignValue, char>
{
public:
    virtual void compute(MessageContainer & messages)
    {}//not used
}
;

struct blk_less
{
    bool operator()(BAssignVertex* const & a, BAssignVertex* const & b) const
    {
        if(a->value().block<b->value().block)
            return true;
        else
            return false;
    }
};


class BAssignWorker
{
    typedef vector<BAssignVertex*> VertexContainer;
    typedef typename VertexContainer::iterator VertexIter;
    typedef BAssignVertex::HashType HashT;
    typedef hash_map<VertexID, VertexID> HashMap;
    typedef HashMap::iterator MapIter;
    typedef MessageBuffer<BAssignVertex> MessageBufT;
    typedef typename MessageBufT::MessageContainerT MessageContainerT;

public:
    HashT hash;
    VertexContainer vertexes;

    HashMap blk2slv;

    BAssignWorker()
    {
        //init_workers();//@@@@@@@@@@@@@@@@@
        global_message_buffer=NULL;
        global_combiner=NULL;
        global_aggregator=NULL;
        global_agg=NULL;
    }

    ~BAssignWorker()
    {
        //worker_finalize();//@@@@@@@@@@@@@@@@@
    }

    void sync_graph()
    {
        //ResetTimer(4);//DEBUG !!!!!!!!!!
        //set send buffer
        vector<vector<BAssignVertex*> > _loaded_parts(_num_workers);
        for(int i=0; i<vertexes.size(); i++)
        {
            BAssignVertex* v=vertexes[i];
            _loaded_parts[hash(v->id)].push_back(v);
        }
        //exchange vertices to add
        all_to_all(_loaded_parts);
        //delete sent vertices
        for(int i=0; i<vertexes.size(); i++)
        {
            BAssignVertex* v=vertexes[i];
            if(hash(v->id)!=_my_rank)
                delete v;
        }
        vertexes.clear();
        //collect vertices to add
        for(int i=0; i<_num_workers; i++)
        {
            vertexes.insert(vertexes.end(), _loaded_parts[i].begin(), _loaded_parts[i].end());
        }
        //===== deprecated ======
        //_my_part.sort(); //no need to sort now
        //StopTimer(4);//DEBUG !!!!!!!!!!
        //PrintTimer("Reduce Time",4);//DEBUG !!!!!!!!!!
    };
    //*/

    //user-defined graphLoader ==============================
    virtual BAssignVertex* toVertex(char* line)=0;//this is what user specifies!!!!!!

    void load_vertex(BAssignVertex* v)
    {//called by load_graph
        vertexes.push_back(v);
    }

    void load_graph(const char* inpath)
    {
        hdfsFS fs = getHdfsFS();
        hdfsFile in=getRHandle(inpath, fs);
        LineReader reader(fs, in);
        while(true)
        {
            reader.readLine();
            if(!reader.eof())
                load_vertex(toVertex(reader.getLine()));
            else
                break;
        }
        hdfsCloseFile(fs, in);
        hdfsDisconnect(fs);
        cout<<"Worker "<<_my_rank<<": \""<<inpath<<"\" loaded"<<endl;//DEBUG !!!!!!!!!!
    }
    //=======================================================

    //user-defined graphDumper ==============================
    virtual void toline(BAssignVertex* v, BufferedWriter & writer)=0;//this is what user specifies!!!!!!
    void dump_partition(const char* outpath)
    {//only writes to one file "part_machineID"
        hdfsFS fs = getHdfsFS();
        BufferedWriter* writer=new BufferedWriter(outpath, fs);

        for(VertexIter it=vertexes.begin(); it!=vertexes.end(); it++)
        {
            writer->check();
            toline(*it, *writer);
        }
        delete writer;
        hdfsDisconnect(fs);
    }

    //=======================================================

    struct sizedBlock
    {
        int block;
        int size;

        bool operator<(const sizedBlock& o) const
        {
            return size>o.size;//large file goes first
        }
    };

    void greedy_assign()
    {
        //collect Voronoi cell size
        HashMap map;
        for(VertexIter it=vertexes.begin(); it!=vertexes.end(); it++)
        {
            VertexID color=(*it)->value().block;
            MapIter mit=map.find(color);
            if(mit==map.end())
                map[color]=1;
            else
                mit->second++;
        }

        //inter-round aggregating
        if(_my_rank!=MASTER_RANK)
        {//send partialT to colorMap
            //%%%%%% gathering colorMap
            slaveGather(map);
            //%%%%%% scattering blk2slv
            slaveBcast(blk2slv);
        }
        else
        {
            //%%%%%% gathering colorMap
            vector<HashMap> parts(_num_workers);
            masterGather(parts);

            for(int i=0; i<_num_workers; i++)
            {
                if(i!=MASTER_RANK)
                {
                    HashMap & part=parts[i];
                    for(MapIter it=part.begin(); it!=part.end(); it++)
                    {
                        VertexID color=it->first;
                        MapIter mit=map.find(color);
                        if(mit==map.end())
                            map[color]=it->second;
                        else
                            mit->second+=it->second;
                    }
                }
            }
            //-------------------------
            vector<sizedBlock> sizedblocks;
            hash_map<int, int> histogram;//%%% for print only %%%
            for(MapIter it=map.begin(); it!=map.end(); it++)
            {
                sizedBlock cur={it->first, it->second};
                sizedblocks.push_back(cur);
                //{ %%% for print only %%%
                int key=numDigits(it->second);
                hash_map<int, int>::iterator hit=histogram.find(key);
                if(hit==histogram.end())
                    histogram[key]=1;
                else
                    hit->second++;
                //%%% for print only %%% }
            }
            sort(sizedblocks.begin(), sizedblocks.end());
            int* assigned=new int[_num_workers];
            int* bcount=new int[_num_workers];//%%% for print only %%%
            for(int i=0; i<_num_workers; i++)
            {
                assigned[i]=0;
                bcount[i]=0;
            }
            for(int i=0; i<sizedblocks.size(); i++)
            {
                int min=0;
                int minSize=assigned[0];
                for(int j=1; j<_num_workers; j++)
                {
                    if(minSize>assigned[j])
                    {
                        min=j;
                        minSize=assigned[j];
                    }
                }
                sizedBlock & cur=sizedblocks[i];
                blk2slv[cur.block]=min;
                bcount[min]++;//%%% for print only %%%
                assigned[min]+=cur.size;
            }
            //------------------- report begin -------------------
            cout<<"* block size histogram:"<<endl;
            for(hash_map<int, int>::iterator hit=histogram.begin(); hit!=histogram.end(); hit++)
            {
                cout<<"|V|<"<<hit->first<<": "<<hit->second<<" blocks"<<endl;
            }
            cout<<"* per-machine block assignment:"<<endl;
            for(int i=0; i<_num_workers; i++)
            {
                cout<<"Machine_"<<i<<" is assigned "<<bcount[i]<<" blocks, "<<assigned[i]<<" vertices"<<endl;
            }
            //------------------- report end ---------------------
            delete[] bcount;
            delete[] assigned;
            //%%%%%% scattering blk2slv
            masterBcast(blk2slv);
        }
    }

    //=======================================================

    int slaveOf(BAssignVertex* v)
    {
        return blk2slv[v->value().block];
    }

    int numDigits(int number)
    {
        int cur=1;
        while(number!=0)
        {
            number/=10;
            cur*=10;
        }
        return cur;
    }

    //=======================================================

    void block_sync()
    {
        //ResetTimer(4);//DEBUG !!!!!!!!!!
        //set send buffer
        vector<vector<BAssignVertex*> > _loaded_parts(_num_workers);
        for(int i=0; i<vertexes.size(); i++)
        {
            BAssignVertex* v=vertexes[i];
            _loaded_parts[slaveOf(v)].push_back(v);
        }
        //exchange vertices to add
        all_to_all(_loaded_parts);
        //delete sent vertices
        for(int i=0; i<vertexes.size(); i++)
        {
            BAssignVertex* v=vertexes[i];
            if(slaveOf(v)!=_my_rank)
                delete v;
        }
        vertexes.clear();
        //collect vertices to add
        for(int i=0; i<_num_workers; i++)
        {
            vertexes.insert(vertexes.end(), _loaded_parts[i].begin(), _loaded_parts[i].end());
        }
        //===== deprecated ======
        sort(vertexes.begin(), vertexes.end(), blk_less()); //groupby block_id
        //StopTimer(4);//DEBUG !!!!!!!!!!
        //PrintTimer("Reduce Time",4);//DEBUG !!!!!!!!!!
    }

    //=======================================================
    typedef vector<hash_map<int, triplet> > MapSet;

    struct IDTrip
    {//remember to free "messages" outside
        VertexID id;
        triplet trip;

        friend ibinstream & operator<<(ibinstream & m, const IDTrip & idm)
        {
            m<<idm.id;
            m<<idm.trip;
            return m;
        }
        friend obinstream & operator>>(obinstream &m, IDTrip & idm)
        {
            m>>idm.id;
            m>>idm.trip;
            return m;
        }
    };

    typedef vector<vector<IDTrip> > ExchangeT;

    void nbInfoExchange()
    {
        ResetTimer(4);
        if(_my_rank==MASTER_RANK)
            cout<<"============= Neighbor InfoExchange Phase 1 (send hash_table) ============="<<endl;
        MapSet maps(_num_workers);
        for(VertexIter it=vertexes.begin(); it!=vertexes.end(); it++)
        {
            VertexID vid=(*it)->id;
            int blockID=(*it)->value().block;
            triplet trip={vid, blockID, blk2slv[blockID]};
            vector<VertexID> & nbs=(*it)->value().neighbors;
            for(int i=0; i<nbs.size(); i++)
            {
                maps[hash(nbs[i])][vid]=trip;
            }
        }
        //////
        ExchangeT recvBuf(_num_workers);
        all_to_all(maps, recvBuf);
        hash_map<VertexID, triplet> & mymap=maps[_my_rank];
        // gather all table entries
        for(int i=0;i<_num_workers;i++)
        {
            if(i!=_my_rank)
            {
                maps[i].clear();//free sent table
                /////
                vector<IDTrip> & entries=recvBuf[i];
                for(int j=0; j<entries.size(); j++)
                {
                    IDTrip & idm=entries[j];
                    mymap[idm.id]=idm.trip;
                }
            }
        }
        //////
        StopTimer(4);
        if(_my_rank==MASTER_RANK)
            cout<<get_timer(4)<<" seconds elapsed"<<endl;
        ResetTimer(4);
        if(_my_rank==MASTER_RANK)
            cout<<"============= Neighbor InfoExchange Phase 2 ============="<<endl;
        for(VertexIter it=vertexes.begin(); it!=vertexes.end(); it++)
        {
            BAssignVertex & vcur=**it;
            vector<VertexID> & nbs=vcur.value().neighbors;
            vector<triplet> & infos=vcur.value().nbsInfo;
            for(int i=0; i<nbs.size(); i++)
            {
                VertexID nb=nbs[i];
                triplet trip=mymap[nb];
                infos.push_back(trip);
            }
        }
        StopTimer(4);
        if(_my_rank==MASTER_RANK)
            cout<<get_timer(4)<<" seconds elapsed"<<endl;
    }

    //=======================================================

    // run the worker
    void run(const WorkerParams & params)
    {
        //check path + init
        if(_my_rank==MASTER_RANK)
        {
            if(dirCheck(params.input_path.c_str(), params.output_path.c_str(), _my_rank==MASTER_RANK, params.force_write)==-1)
                return;
        }
        init_timers();

        //dispatch splits
        ResetTimer(WORKER_TIMER);
        vector<vector<string> > * arrangement;
        if(_my_rank==MASTER_RANK)
        {
            arrangement=params.native_dispatcher ? dispatchLocality(params.input_path.c_str()) : dispatchRan(params.input_path.c_str());
            //reportAssignment(arrangement);//DEBUG !!!!!!!!!!
            masterScatter(*arrangement);
            vector<string> & assignedSplits=(*arrangement)[0];
            //reading assigned splits (map)
            for(vector<string>::iterator it=assignedSplits.begin();
                    it!=assignedSplits.end(); it++)
                load_graph(it->c_str());
            delete arrangement;
        }
        else
        {
            vector<string> assignedSplits;
            slaveScatter(assignedSplits);
            //reading assigned splits (map)
            for(vector<string>::iterator it=assignedSplits.begin();
                    it!=assignedSplits.end(); it++)
                load_graph(it->c_str());
        }

        //send vertices according to hash_id (reduce)
        sync_graph();

        //barrier for data loading
        //worker_barrier();
        StopTimer(WORKER_TIMER);
        PrintTimer("Load Time",WORKER_TIMER);

        //=========================================================

        init_timers();
        ResetTimer(WORKER_TIMER);

        if(_my_rank==MASTER_RANK)
            cout<<"============ GREEDY blk2slv ASSIGNMENT ============"<<endl;
        ResetTimer(4);
        greedy_assign();
        StopTimer(4);
        if(_my_rank==MASTER_RANK)
        {
            cout<<"============ GREEDY blk2slv DONE ============"<<endl;
            cout<<"Time elapsed: "<<get_timer(4)<<" seconds"<<endl;
        }

        //=========================================================

        nbInfoExchange();

        //=========================================================

        if(_my_rank==MASTER_RANK)
            cout<<"SYNC VERTICES ......"<<endl;
        ResetTimer(4);
        block_sync();
        StopTimer(4);
        if(_my_rank==MASTER_RANK)
            cout<<"SYNC done in "<<get_timer(4)<<" seconds"<<endl;
        //=========================================================

        //worker_barrier();
        StopTimer(WORKER_TIMER);
        PrintTimer("Communication Time", COMMUNICATION_TIMER);
        PrintTimer("- Serialization Time", SERIALIZATION_TIMER);
        PrintTimer("- Transfer Time", TRANSFER_TIMER);
        PrintTimer("Total Computational Time",WORKER_TIMER);
        // dump graph
        ResetTimer(WORKER_TIMER);
        char* fpath=new char[params.output_path.length()+10];
        strcpy(fpath, params.output_path.c_str());
        strcat(fpath, "/part_");
        char buffer[5];
        sprintf(buffer, "%d", _my_rank);
        strcat(fpath, buffer);
        dump_partition(fpath);
        delete fpath;
        StopTimer(WORKER_TIMER);
        PrintTimer("Dump Time",WORKER_TIMER);
    }
};

#endif
