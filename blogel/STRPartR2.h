#ifndef STRPARTR2_H_
#define STRPARTR2_H_

#include "BVertex.h"
#include "Block.h"
#include "VMessageBuffer.h"
#include "BType.h"
//-----------------
#include "utils/serialization.h"
#include "utils/communication.h"
#include "utils/ydhdfs.h"

#include "basic/Worker.h"
#include "BGlobal.h"
#include <vector>
#include <iostream>
#include <ext/hash_map>
#include <queue>

struct STR2Value {
    int new_bid;
    int split;
    vector<triplet> neighbors;
    string content;
};

//--------------------------------------------------

class STR2Vertex : public BVertex<VertexID, STR2Value, triplet> {
    //key: vertex id
    //value: STR2Value
    //msg: triplet for nbInfoExchange
public:
    virtual void compute(MessageContainer& messages)
    {
    } //dummy

    void broadcast(triplet msg)
    {
        vector<triplet>& nbs = value().neighbors;
        for (int i = 0; i < nbs.size(); i++) {
            send_message(nbs[i].vid, nbs[i].wid, msg);
        }
    }
};

//--------------------------------------------------

class STR2Block : public Block<char, STR2Vertex, char> {
public:
    virtual void compute(MessageContainer& messages, VertexContainer& vertexes)
    {
    } //dummy
};

//--------------------------------------------------

//Round 2: BFS(each super-block)
//- after BFS, update block info

//superstep 1: compute BFS, set new_blk_id (b.compute())
//superstep 2: broadcasst new_blk_id (v.compute())
//superstep 3: update nb_info (v.compute())

class STR2Worker {
public:
    typedef vector<STR2Vertex*> VertexContainer;
    typedef VertexContainer::iterator VertexIter;
    typedef vector<STR2Block*> BlockContainer;
    typedef BlockContainer::iterator BlockIter;
    typedef VMessageBuffer<STR2Vertex> VMessageBufT;
    typedef VMessageBufT::MessageContainerT MessageContainerT;

    VertexContainer vertexes;
    VMessageBuffer<STR2Vertex>* vmessage_buffer;
    //---------------------
    BlockContainer blocks;
    //---------------------
    enum DUMPMODE {
        B_DUMP = 0,
        V_DUMP = 1
    };
    int dump_mode;
    void set_dump_mode(int mode)
    {
        dump_mode = mode;
    }

    //===================================

    STR2Worker()
    {
        curBlkID = 0;
        //init_workers();//@@@@@@@@@@@@@@@@@@@@@@@@
        ///////////////
        vmessage_buffer = new VMessageBuffer<STR2Vertex>;
        global_message_buffer = vmessage_buffer;
        ///////////////
        dump_mode = B_DUMP;
    }

    virtual ~STR2Worker()
    {
        for (VertexIter it = vertexes.begin(); it != vertexes.end(); it++)
            delete *it;
        for (BlockIter it = blocks.begin(); it != blocks.end(); it++)
            delete *it;
        delete vmessage_buffer;
        //worker_finalize();//@@@@@@@@@@@@@@@@@@@@@@@@
    }

    //user-defined graphLoader ==============================
    virtual STR2Vertex* toVertex(char* line) = 0; //this is what user specifies!!!!!!

    void load_vertex(STR2Vertex* v)
    { //called by load_graph
        vertexes.push_back(v);
    }

    void load_graph(const char* inpath)
    {
        hdfsFS fs = getHdfsFS();
        hdfsFile in = getRHandle(inpath, fs);
        LineReader reader(fs, in);
        while (true) {
            reader.readLine();
            if (!reader.eof())
                load_vertex(toVertex(reader.getLine()));
            else
                break;
        }
        hdfsCloseFile(fs, in);
        hdfsDisconnect(fs);
        //cout << "Worker " << _my_rank << ": \"" << inpath << "\" loaded" << endl; //DEBUG !!!!!!!!!!
    }
    //=======================================================

    //user-defined graphDumper ==============================
    virtual void toline(STR2Block* b, STR2Vertex* v, BufferedWriter& writer) = 0; //this is what user specifies!!!!!!
    void vdump(const char* outpath)
    {
        hdfsFS fs = getHdfsFS();
        BufferedWriter* writer = new BufferedWriter(outpath, fs, _my_rank);

        for (BlockIter it = blocks.begin(); it != blocks.end(); it++) {
            STR2Block* block = *it;
            for (int i = block->begin; i < block->begin + block->size; i++) {
                writer->check();
                toline(block, vertexes[i], *writer);
            }
        }
        delete writer;
        hdfsDisconnect(fs);
    }

    void bdump(const char* outpath)
    {
        hdfsFS fs = getHdfsFS();
        BufferedWriter* writer = new BufferedWriter(outpath, fs);

        for (BlockIter it = blocks.begin(); it != blocks.end(); it++) {
            STR2Block* block = *it;
            for (int i = block->begin; i < block->begin + block->size; i++) {
                writer->check();
                toline(block, vertexes[i], *writer);
            }
        }
        delete writer;
        hdfsDisconnect(fs);
    }
    //=======================================================

    void blockInit()
    {
        hash_map<int, int> map;
        for (int i = 0; i < vertexes.size(); i++)
            map[vertexes[i]->id] = i;
        //////
        if (_my_rank == MASTER_RANK)
            cout << "Splitting in/out-superblock edges ..." << endl;
        for (BlockIter it = blocks.begin(); it != blocks.end(); it++) {
            STR2Block* block = *it;
            for (int i = 0; i < block->size; i++) {
                int pos = block->begin + i;
                STR2Vertex* vertex = vertexes[pos];
                vector<triplet>& edges = vertex->value().neighbors;
                vector<triplet> tmp;
                vector<triplet> tmp1;
                for (int j = 0; j < edges.size(); j++) {
                    if (edges[j].bid == block->bid) {
                        edges[j].wid = map[edges[j].vid]; //workerID->array index
                        tmp.push_back(edges[j]);
                    } else
                        tmp1.push_back(edges[j]);
                }
                edges.swap(tmp);
                vertex->value().split = edges.size() - 1;
                edges.insert(edges.end(), tmp1.begin(), tmp1.end());
            }
        }
        if (_my_rank == MASTER_RANK)
            cout << "In/out-superblock edges split" << endl;
    }

    //=======================================================
    //superstep 1: compute DFS, set new_blk_id (b.compute())
    int curBlkID;

    void BFS(int logID, STR2Block* block, bool* visited)
    {
        queue<int> q;
        q.push(logID);
        visited[logID] = true;
        while (!q.empty()) {
            logID = q.front();
            q.pop();

            int phyID = block->begin + logID;
            STR2Vertex* vertex = vertexes[phyID];
            //////////////////
            vertex->value().new_bid = curBlkID;

            //////////////////
            vector<triplet>& edges = vertex->value().neighbors;
            int split = vertex->value().split;
            for (int i = 0; i <= split; i++) {
                triplet nb = edges[i];
                int physicID = nb.wid;
                int logicID = physicID - block->begin;
                if (visited[logicID] == false) {
                    visited[logicID] = true;
                    q.push(logicID);
                }
            }
        }
    }

    struct strblk_less {
        bool operator()(STR2Vertex* const& a, STR2Vertex* const& b) const
        {
            if (a->value().new_bid < b->value().new_bid)
                return true;
            else
                return false;
        }
    };

    void superstep1()
    {
        ResetTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << "============= Superstep 1: BFS blk-relabeling =============" << endl;
        for (BlockIter it = blocks.begin(); it != blocks.end(); it++) {
            STR2Block* block = *it;
            int num = block->size;
            bool* visited = new bool[num];
            for (int i = 0; i < num; i++)
                visited[i] = false; //init visited
            for (int i = 0; i < block->size; i++) {
                if (visited[i] == false) {
                    BFS(i, block, visited);
                    curBlkID++;
                }
            }
            delete visited;
            //sort vertices by new blockID
            sort(vertexes.begin() + block->begin, vertexes.begin() + block->begin + block->size, strblk_less());
        }
        cout << _my_rank << ": num_blocks=" << curBlkID << endl;
        StopTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << get_timer(4) << " seconds elapsed" << endl;
    }

    void recoverWorkers()
    {
        //recover localVertex.worker field, from local index in "vertexes" to workerID
        //this must be done for supersteps 2 and 3 to be correctly run in vmode
        for (int i = 0; i < vertexes.size(); i++) {
            STR2Vertex* vertex = vertexes[i];
            vector<triplet>& edges = vertex->value().neighbors;
            int split = vertex->value().split;
            for (int i = 0; i <= split; i++) {
                edges[i].wid = _my_rank;
            }
        }
    }

    //=======================================================
    //between superstep 1 and 2
    //- get prefixes
    //- add prefix to new-blk-ID
    int prefix;

    void getPrefix()
    {
        //curBlkID = # of new blocks on local machine
        if (_my_rank == MASTER_RANK) {
            vector<int> count(_num_workers);
            masterGather(count);
            vector<int> pref(_num_workers);
            pref[0] = 0;
            for (int i = 1; i < _num_workers; i++) {
                pref[i] = pref[i - 1] + count[i - 1];
            }
            //----
            prefix = pref[MASTER_RANK];
            masterScatter(pref);
        } else {
            slaveGather(curBlkID);
            slaveScatter(prefix);
        }
    }

    void shiftBlkId()
    {
        for (VertexIter it = vertexes.begin(); it != vertexes.end(); it++) {
            STR2Vertex* vertex = *it;
            vertex->value().new_bid += prefix;
        }
    }

    //=======================================================
    //superstep 2: broadcasst new_blk_id (v.compute())
    //superstep 3: update nb_info (v.compute())
    void superstep2_3()
    {
        ResetTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << "=============  Superstep 2: broadcast own new_blk_id =============" << endl;
        for (VertexIter it = vertexes.begin(); it != vertexes.end(); it++) {
            triplet msg = {(*it)->id, (*it)->value().new_bid, (*it)->wid };
            (*it)->broadcast(msg);
        }
        vmessage_buffer->sync_messages();
        StopTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << get_timer(4) << " seconds elapsed" << endl;
        ResetTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << "============= Superstep 3: collect \"new_blk_id\"s =============" << endl;
        VMessageBufT* mbuf = (VMessageBufT*)get_message_buffer();
        vector<MessageContainerT>& v_msgbufs = mbuf->get_v_msg_bufs();
        for (int i = 0; i < vertexes.size(); i++) {
            MessageContainerT& nbs = vertexes[i]->value().neighbors;
            nbs.swap(v_msgbufs[i]);
            v_msgbufs[i].clear(); //clear used msgs
        }
        StopTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << get_timer(4) << " seconds elapsed" << endl;
    }

    //=======================================================
    // run the worker
    void run(const WorkerParams& params)
    {
        //check path + init
        if (_my_rank == MASTER_RANK) {
            if (dirCheck(params.input_path.c_str(), params.output_path.c_str(), _my_rank == MASTER_RANK, params.force_write) == -1)
                return;
        }
        init_timers();

        //------------------------
        ResetTimer(WORKER_TIMER);
        char tmp[5];
        sprintf(tmp, "%d", _my_rank);
        string myfile = params.input_path + "/part_" + tmp;
        load_graph(myfile.c_str());
        //barrier for data loading
        worker_barrier();
        StopTimer(WORKER_TIMER);
        PrintTimer("Load Time", WORKER_TIMER);

        //=========================================================

        //-------- create blocks ----------
        int prev = -1;
        STR2Block* block = NULL;
        int pos;
        for (pos = 0; pos < vertexes.size(); pos++) {
            int bid = vertexes[pos]->bid;
            if (bid != prev) {
                if (block != NULL) {
                    block->size = pos - block->begin;
                    blocks.push_back(block);
                }
                block = new STR2Block;
                prev = block->bid = bid;
                block->begin = pos;
            }
        }
        //flush
        if (block != NULL) {
            block->size = pos - block->begin;
            blocks.push_back(block);
        }
        //----
        blockInit();

        //=========================================================

        init_timers();
        ResetTimer(WORKER_TIMER);

        //supersteps
        superstep1();
        vmessage_buffer->init(vertexes); //must be called here, as superstep1 sorts "vertexes"
        recoverWorkers();
        getPrefix();
        shiftBlkId();
        superstep2_3();

        worker_barrier();
        StopTimer(WORKER_TIMER);
        PrintTimer("Communication Time", COMMUNICATION_TIMER);
        PrintTimer("- Serialization Time", SERIALIZATION_TIMER);
        PrintTimer("- Transfer Time", TRANSFER_TIMER);
        PrintTimer("Total Computational Time", WORKER_TIMER);
        ;

        // dump graph
        ResetTimer(WORKER_TIMER);
        if (dump_mode == V_DUMP)
            vdump(params.output_path.c_str());
        else {
            string outfile = params.output_path + "/part_" + tmp;
            bdump(outfile.c_str()); //dump_mode==B_DUMP
        }
        worker_barrier();
        StopTimer(WORKER_TIMER);
        PrintTimer("Dump Time", WORKER_TIMER);
    }
};

#endif
