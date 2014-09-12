#ifndef VORONOI_H_
#define VORONOI_H_

#include "utils/serialization.h"
#include "utils/communication.h"
#include "utils/Combiner.h"
#include "utils/ydhdfs.h"
#include "basic/Vertex.h"
#include "BGlobal.h"
#include "BType.h"
#include <vector>
#include <iostream>
#include <stdlib.h>
#include <ext/hash_map>

struct BPartValue {
    VertexID color; //block ID
    vector<VertexID> neighbors;
    vector<triplet> nbsInfo;
    vector<VertexID> content;

    BPartValue()
        : color(-1)
    {
    }
};

//no vertex-add, will not be called
ibinstream& operator<<(ibinstream& m, const BPartValue& v)
{
    m << v.color;
    m << v.neighbors;
    m << v.nbsInfo;
    m << v.content;
    return m;
}

//no vertex-add, will not be called
obinstream& operator>>(obinstream& m, BPartValue& v)
{
    m >> v.color;
    m >> v.neighbors;
    m >> v.nbsInfo;
    m >> v.content;
    return m;
}

class BPartVertex : public Vertex<VertexID, BPartValue, VertexID> {
public:
    BPartVertex()
    {
        srand((unsigned)time(NULL));
    }

    void broadcast(VertexID msg)
    {
        vector<VertexID>& nbs = value().neighbors;
        for (int i = 0; i < nbs.size(); i++) {
            send_message(nbs[i], msg);
        }
    }

    virtual void compute(MessageContainer& messages) //Voronoi Diagram partitioning algorithm
    {
        if (step_num() == 1) {
            double samp = ((double)rand()) / RAND_MAX;
            if (samp <= global_sampling_rate) { //sampled
                value().color = id;
                broadcast(id);
            } else { //not sampled
                value().color = -1; //-1 means not assigned color
            }
            vote_to_halt();
        } else if (step_num() >= global_max_hop)
            vote_to_halt();
        else {
            if (value().color == -1) {
                VertexID msg = *(messages.begin());
                value().color = msg;
                broadcast(msg);
            }
            vote_to_halt();
        }
    }
};

class BPartVorCombiner : public Combiner<VertexID> {
public:
    virtual void combine(VertexID& old, const VertexID& new_msg)
    {
    } //ignore new_msg
};

class BPartHashMinCombiner : public Combiner<VertexID> {
public:
    virtual void combine(VertexID& old, const VertexID& new_msg)
    {
        if (old > new_msg)
            old = new_msg;
    }
};

struct blk_less_vor {
    bool operator()(BPartVertex* const& a, BPartVertex* const& b) const
    {
        if (a->value().color < b->value().color)
            return true;
        else
            return false;
    }
};

class BPartWorker {
    typedef vector<BPartVertex*> VertexContainer;
    typedef VertexContainer::iterator VertexIter;
    typedef BPartVertex::HashType HashT;
    typedef hash_map<VertexID, VertexID> HashMap;
    typedef HashMap::iterator MapIter;
    typedef MessageBuffer<BPartVertex> MessageBufT;
    typedef MessageBufT::MessageContainerT MessageContainerT;

public:
    HashT hash;
    VertexContainer vertexes;
    int active_count;
    HashMap blk2slv;
    MessageBuffer<BPartVertex>* message_buffer;
    Combiner<VertexID>* combiner;

    BPartWorker()
    {
        //init_workers();//@@@@@@@@@@@@@@@@@@@@@
        message_buffer = new MessageBuffer<BPartVertex>;
        global_message_buffer = message_buffer;
        combiner = NULL;
        global_combiner = NULL;
        global_aggregator = NULL;
        global_agg = NULL;
    }

    virtual ~BPartWorker()
    {
        //worker_finalize();//@@@@@@@@@@@@@@@@@@@@@
        delete message_buffer;
    }

    void setVoronoiCombiner() //only for the part of logic about Voronoi sampling
    {
        combiner = new BPartVorCombiner;
        global_combiner = combiner;
    }

    void setHashMinCombiner() //only for the part of logic about Voronoi sampling
    {
        combiner = new BPartHashMinCombiner;
        global_combiner = combiner;
    }

    void unsetCombiner() //must call after Voronoi sampling
    {
        global_combiner = NULL;
        delete combiner;
        combiner = NULL;
    }

    void sync_graph()
    {
        //ResetTimer(4);//DEBUG !!!!!!!!!!
        //set send buffer
        vector<vector<BPartVertex*> > _loaded_parts(_num_workers);
        for (int i = 0; i < vertexes.size(); i++) {
            BPartVertex* v = vertexes[i];
            _loaded_parts[hash(v->id)].push_back(v);
        }
        //exchange vertices to add
        all_to_all(_loaded_parts);
        //delete sent vertices
        for (int i = 0; i < vertexes.size(); i++) {
            BPartVertex* v = vertexes[i];
            if (hash(v->id) != _my_rank)
                delete v;
        }
        vertexes.clear();
        //collect vertices to add
        for (int i = 0; i < _num_workers; i++) {
            vertexes.insert(vertexes.end(), _loaded_parts[i].begin(), _loaded_parts[i].end());
        }
        //===== deprecated ======
        //_my_part.sort(); //no need to sort now
        _loaded_parts.clear();
        //StopTimer(4);//DEBUG !!!!!!!!!!
        //PrintTimer("Reduce Time",4);//DEBUG !!!!!!!!!!
    };
    //*/

    //user-defined graphLoader ==============================
    virtual BPartVertex* toVertex(char* line) = 0; //this is what user specifies!!!!!!

    void load_vertex(BPartVertex* v)
    { //called by load_graph
        vertexes.push_back(v);
        if (v->is_active()) {
            active_count += 1;
        }
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
        //        cout << "Worker " << _my_rank << ": \"" << inpath << "\" loaded" << endl; //DEBUG !!!!!!!!!!
    }
    //=======================================================

    //user-defined graphDumper ==============================

    virtual void toline(BPartVertex* v, BufferedWriter& writer) = 0; //this is what user specifies!!!!!!
    void dump_partition(const char* outpath)
    { //only writes to one file "part_machineID"
        hdfsFS fs = getHdfsFS();
        BufferedWriter* writer = new BufferedWriter(outpath, fs);

        for (VertexIter it = vertexes.begin(); it != vertexes.end(); it++) {
            writer->check();
            toline(*it, *writer);
        }
        delete writer;
        hdfsDisconnect(fs);
    }
    //=======================================================

    void to_undirected()
    {
        ResetTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << "============= To_Directed Phase 1 =============" << endl;
        for (VertexIter it = vertexes.begin(); it != vertexes.end(); it++) {
            (*it)->broadcast((*it)->id);
        }
        message_buffer->sync_messages();
        StopTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << get_timer(4) << " seconds elapsed" << endl;
        ResetTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << "============= To_Directed Phase 2 =============" << endl;
        MessageBufT* mbuf = (MessageBufT*)get_message_buffer();
        vector<MessageContainerT>& v_msgbufs = mbuf->get_v_msg_bufs();
        for (int i = 0; i < vertexes.size(); i++) {
            hash_set<VertexID> nbs;
            for (int j = 0; j < v_msgbufs[i].size(); j++)
                nbs.insert(v_msgbufs[i][j]);
            v_msgbufs[i].clear(); //clear used msgs
            vector<VertexID>& nbs1 = vertexes[i]->value().neighbors;
            for (int i = 0; i < nbs1.size(); i++)
                nbs.insert(nbs1[i]);
            nbs1.clear();
            for (hash_set<VertexID>::iterator sit = nbs.begin(); sit != nbs.end(); sit++)
                nbs1.push_back(*sit);
        }
        StopTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << get_timer(4) << " seconds elapsed" << endl;
        clearBits();
    }

    //=======================================================

    void voronoi_compute()
    { //returns true if all colored && no overflowing Voronoi cell
        //collect Voronoi cell size
        HashMap map;
        for (VertexIter it = vertexes.begin(); it != vertexes.end(); it++) {
            VertexID color = (*it)->value().color;
            if (color != -1) {
                MapIter mit = map.find(color);
                if (mit == map.end())
                    map[color] = 1;
                else
                    mit->second++;
            }
        }

        //colorMap aggregating
        if (_my_rank != MASTER_RANK) { //send partialT to aggregator
            //%%%%%% gathering colorMap
            slaveGather(map);
            //%%%%%% scattering FinalT
            slaveBcast(map);
        } else {
            //%%%%%% gathering colorMap
            vector<HashMap> parts(_num_workers);
            masterGather(parts);

            for (int i = 0; i < _num_workers; i++) {
                if (i != MASTER_RANK) {
                    HashMap& part = parts[i];
                    for (MapIter it = part.begin(); it != part.end(); it++) {
                        VertexID color = it->first;
                        MapIter mit = map.find(color);
                        if (mit == map.end())
                            map[color] = it->second;
                        else
                            mit->second += it->second;
                    }
                }
            }
            //%%%%%% scattering FinalT
            masterBcast(map);
        }

        //===============================================
        active_count = 0;
        for (VertexIter it = vertexes.begin(); it != vertexes.end(); it++) {
            VertexID color = (*it)->value().color;
            if (color == -1) //case 1: not colored
            {
                (*it)->activate();
                active_count++;
            } else {
                MapIter mit = map.find(color);

                if (mit->second > global_max_vcsize) { //case 2: Voronoi cell size overflow
                    (*it)->activate();
                    (*it)->value().color = -1; //erase old color
                    active_count++;
                }
            }
        }
    }

    //=======================================================

    void subGHashMin()
    {
        if (_my_rank == MASTER_RANK)
            cout << "-------------------------- Switch to subgraph HashMin --------------------------" << endl;
        setHashMinCombiner();
        //adapted HashMin: only works on the subgraph induced by active vertices
        int size = vertexes.size();
        VertexContainer subgraph; //record subgraph vertices
        for (int i = 0; i < size; i++) {
            if (vertexes[i]->is_active())
                subgraph.push_back(vertexes[i]);
        }
        global_step_num = 0;
        while (true) {
            global_step_num++;
            ResetTimer(4);
            //===================
            char bits_bor = all_bor(global_bor_bitmap);
            active_vnum() = all_sum(active_count);
            if (active_vnum() == 0 && getBit(HAS_MSG_ORBIT, bits_bor) == 0)
                break;
            //===================
            clearBits();
            //-------------------
            active_count = 0;
            MessageBufT* mbuf = (MessageBufT*)get_message_buffer();
            mbuf->reinit(subgraph);
            vector<MessageContainerT>& v_msgbufs = mbuf->get_v_msg_bufs();
            for (int pos = 0; pos < subgraph.size(); pos++) {
                BPartVertex& v = *subgraph[pos];
                if (step_num() == 1) {
                    //not all neighbors are in subgraph
                    //cannot init using min{neighbor_id}
                    VertexID min = v.id;
                    v.value().color = min;
                    v.broadcast(v.id);
                    v.vote_to_halt();
                } else {
                    if (v_msgbufs[pos].size() > 0) {
                        vector<VertexID>& messages = v_msgbufs[pos];
                        VertexID min = messages[0];
                        for (int i = 1; i < messages.size(); i++) {
                            if (min > messages[i])
                                min = messages[i];
                        }
                        if (min < v.value().color) {
                            v.value().color = min;
                            v.broadcast(min);
                        }
                        v_msgbufs[pos].clear(); //clear used msgs
                    }
                    v.vote_to_halt();
                }
            }
            //-------------------
            message_buffer->sync_messages();
            //===================
            //worker_barrier();
            StopTimer(4);
            if (_my_rank == MASTER_RANK)
                cout << "Superstep " << global_step_num << " done. Time elapsed: " << get_timer(4) << " seconds" << endl;
        }
        unsetCombiner(); //stop subgraph HashMin
        if (_my_rank == MASTER_RANK)
            cout << "-------------------------- Subgraph HashMin END --------------------------" << endl;
    }

    //=======================================================

    struct sizedBlock {
        int color;
        int size;

        bool operator<(const sizedBlock& o) const
        {
            return size > o.size; //large file goes first
        }
    };

    void greedy_assign()
    {
        //collect Voronoi cell size
        HashMap map;
        for (VertexIter it = vertexes.begin(); it != vertexes.end(); it++) {
            VertexID color = (*it)->value().color;
            MapIter mit = map.find(color);
            if (mit == map.end())
                map[color] = 1;
            else
                mit->second++;
        }

        //inter-round aggregating
        if (_my_rank != MASTER_RANK) { //send partialT to colorMap
            //%%%%%% gathering colorMap
            slaveGather(map);
            //%%%%%% scattering blk2slv
            slaveBcast(blk2slv);
        } else {
            //%%%%%% gathering colorMap
            vector<HashMap> parts(_num_workers);
            masterGather(parts);

            for (int i = 0; i < _num_workers; i++) {
                if (i != MASTER_RANK) {
                    HashMap& part = parts[i];
                    for (MapIter it = part.begin(); it != part.end(); it++) {
                        VertexID color = it->first;
                        MapIter mit = map.find(color);
                        if (mit == map.end())
                            map[color] = it->second;
                        else
                            mit->second += it->second;
                    }
                }
            }
            //-------------------------
            vector<sizedBlock> sizedblocks;
            hash_map<int, int> histogram; //%%% for print only %%%
            for (MapIter it = map.begin(); it != map.end(); it++) {
                sizedBlock cur = { it->first, it->second };
                sizedblocks.push_back(cur);
                //{ %%% for print only %%%
                int key = numDigits(it->second);
                hash_map<int, int>::iterator hit = histogram.find(key);
                if (hit == histogram.end())
                    histogram[key] = 1;
                else
                    hit->second++;
                //%%% for print only %%% }
            }
            sort(sizedblocks.begin(), sizedblocks.end());
            int* assigned = new int[_num_workers];
            int* bcount = new int[_num_workers]; //%%% for print only %%%
            for (int i = 0; i < _num_workers; i++) {
                assigned[i] = 0;
                bcount[i] = 0;
            }
            for (int i = 0; i < sizedblocks.size(); i++) {
                int min = 0;
                int minSize = assigned[0];
                for (int j = 1; j < _num_workers; j++) {
                    if (minSize > assigned[j]) {
                        min = j;
                        minSize = assigned[j];
                    }
                }
                sizedBlock& cur = sizedblocks[i];
                blk2slv[cur.color] = min;
                bcount[min]++; //%%% for print only %%%
                assigned[min] += cur.size;
            }
            //------------------- report begin -------------------
            cout << "* block size histogram:" << endl;
            for (hash_map<int, int>::iterator hit = histogram.begin(); hit != histogram.end(); hit++) {
                cout << "|V|<" << hit->first << ": " << hit->second << " blocks" << endl;
            }
            cout << "* per-machine block assignment:" << endl;
            for (int i = 0; i < _num_workers; i++) {
                cout << "Machine_" << i << " is assigned " << bcount[i] << " blocks, " << assigned[i] << " vertices" << endl;
            }
            //------------------- report end ---------------------
            delete[] bcount;
            delete[] assigned;
            //%%%%%% scattering blk2slv
            masterBcast(blk2slv);
        }
    }

    //=======================================================

    int slaveOf(BPartVertex* v)
    {
        return blk2slv[v->value().color];
    }

    int numDigits(int number)
    {
        int cur = 1;
        while (number != 0) {
            number /= 10;
            cur *= 10;
        }
        return cur;
    }

    //=======================================================

    void block_sync()
    {
        //ResetTimer(4);//DEBUG !!!!!!!!!!
        //set send buffer
        vector<vector<BPartVertex*> > _loaded_parts(_num_workers);
        for (int i = 0; i < vertexes.size(); i++) {
            BPartVertex* v = vertexes[i];
            _loaded_parts[slaveOf(v)].push_back(v);
        }
        //exchange vertices to add
        all_to_all(_loaded_parts);
        //delete sent vertices
        for (int i = 0; i < vertexes.size(); i++) {
            BPartVertex* v = vertexes[i];
            if (slaveOf(v) != _my_rank)
                delete v;
        }
        vertexes.clear();
        //collect vertices to add
        for (int i = 0; i < _num_workers; i++) {
            vertexes.insert(vertexes.end(), _loaded_parts[i].begin(), _loaded_parts[i].end());
        }
        //===== deprecated ======
        sort(vertexes.begin(), vertexes.end(), blk_less_vor()); //groupby block_id
        _loaded_parts.clear();
        //StopTimer(4);//DEBUG !!!!!!!!!!
        //PrintTimer("Reduce Time",4);//DEBUG !!!!!!!!!!
    }

    //=======================================================
    typedef vector<hash_map<int, triplet> > MapSet;

    struct IDTrip { //remember to free "messages" outside
        VertexID id;
        triplet trip;

        friend ibinstream& operator<<(ibinstream& m, const IDTrip& idm)
        {
            m << idm.id;
            m << idm.trip;
            return m;
        }
        friend obinstream& operator>>(obinstream& m, IDTrip& idm)
        {
            m >> idm.id;
            m >> idm.trip;
            return m;
        }
    };

    typedef vector<vector<IDTrip> > ExchangeT;

    void nbInfoExchange()
    {
        ResetTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << "============= Neighbor InfoExchange Phase 1 (send hash_table) =============" << endl;
        MapSet maps(_num_workers);
        for (VertexIter it = vertexes.begin(); it != vertexes.end(); it++) {
            VertexID vid = (*it)->id;
            int blockID = (*it)->value().color;
            triplet trip = { vid, blockID, blk2slv[blockID] };
            vector<VertexID>& nbs = (*it)->value().neighbors;
            for (int i = 0; i < nbs.size(); i++) {
                maps[hash(nbs[i])][vid] = trip;
            }
        }
        //////
        ExchangeT recvBuf(_num_workers);
        all_to_all(maps, recvBuf);
        hash_map<VertexID, triplet>& mymap = maps[_my_rank];
        // gather all table entries
        for (int i = 0; i < _num_workers; i++) {
            if (i != _my_rank) {
                maps[i].clear(); //free sent table
                /////
                vector<IDTrip>& entries = recvBuf[i];
                for (int j = 0; j < entries.size(); j++) {
                    IDTrip& idm = entries[j];
                    mymap[idm.id] = idm.trip;
                }
            }
        }
        //////
        StopTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << get_timer(4) << " seconds elapsed" << endl;
        ResetTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << "============= Neighbor InfoExchange Phase 2 =============" << endl;
        for (VertexIter it = vertexes.begin(); it != vertexes.end(); it++) {
            BPartVertex& vcur = **it;
            vector<VertexID>& nbs = vcur.value().neighbors;
            vector<triplet>& infos = vcur.value().nbsInfo;
            for (int i = 0; i < nbs.size(); i++) {
                VertexID nb = nbs[i];
                triplet trip = mymap[nb];
                infos.push_back(trip);
            }
        }
        StopTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << get_timer(4) << " seconds elapsed" << endl;
    }

    //=======================================================

    void active_compute()
    {
        active_count = 0;
        MessageBufT* mbuf = (MessageBufT*)get_message_buffer();
        vector<MessageContainerT>& v_msgbufs = mbuf->get_v_msg_bufs();
        for (int i = 0; i < vertexes.size(); i++) {
            if (v_msgbufs[i].size() == 0) {
                if (vertexes[i]->is_active()) {
                    vertexes[i]->compute(v_msgbufs[i]);
                    if (vertexes[i]->is_active())
                        active_count++;
                }
            } else {
                vertexes[i]->activate();
                vertexes[i]->compute(v_msgbufs[i]);
                v_msgbufs[i].clear(); //clear used msgs
                if (vertexes[i]->is_active())
                    active_count++;
            }
        }
    }

    //=======================================================

    // run the worker
    void run(const WorkerParams& params, bool toUG)
    {
        //check path + init
        if (_my_rank == MASTER_RANK) {
            if (dirCheck(params.input_path.c_str(), params.output_path.c_str(), _my_rank == MASTER_RANK, params.force_write) == -1)
                return;
        }
        init_timers();

        //dispatch splits
        ResetTimer(WORKER_TIMER);
        vector<vector<string> >* arrangement;
        if (_my_rank == MASTER_RANK) {
            arrangement = params.native_dispatcher ? dispatchLocality(params.input_path.c_str()) : dispatchRan(params.input_path.c_str());
            //reportAssignment(arrangement);//DEBUG !!!!!!!!!!
            masterScatter(*arrangement);
            vector<string>& assignedSplits = (*arrangement)[0];
            //reading assigned splits (map)
            for (vector<string>::iterator it = assignedSplits.begin();
                 it != assignedSplits.end(); it++)
                load_graph(it->c_str());
            delete arrangement;
        } else {
            vector<string> assignedSplits;
            slaveScatter(assignedSplits);
            //reading assigned splits (map)
            for (vector<string>::iterator it = assignedSplits.begin();
                 it != assignedSplits.end(); it++)
                load_graph(it->c_str());
        }

        //send vertices according to hash_id (reduce)
        sync_graph();

        //barrier for data loading
        worker_barrier();
        StopTimer(WORKER_TIMER);
        PrintTimer("Load Time", WORKER_TIMER);
        message_buffer->init(vertexes);
        //=========================================================

        init_timers();
        ResetTimer(WORKER_TIMER);
        //toundirected
        if (toUG)
            to_undirected();

        if (_my_rank == MASTER_RANK)
            cout << "-------------------------- Voronoi Partitioning BEGIN --------------------------" << endl;
        setVoronoiCombiner(); //start Voronoi sampling
        get_vnum() = all_sum(vertexes.size());
        int lastNum = get_vnum();
        if (_my_rank == MASTER_RANK)
            cout << "* |V| = " << lastNum << endl;

        //supersteps
        int round = 1;
        if (_my_rank == MASTER_RANK)
            cout << "==================== Round " << round << " ====================" << endl;
        global_step_num = 0;

        while (true) {
            global_step_num++;
            ResetTimer(4);
            //===================
            char bits_bor = all_bor(global_bor_bitmap);
            active_vnum() = all_sum(active_count);
            if (round > 1 && global_step_num == 1) { //{ reporting
                if (active_vnum() == 0) {
                    if (_my_rank == MASTER_RANK)
                        cout << "-------------------------- Voronoi Partitioning END --------------------------" << endl;
                    if (_my_rank == MASTER_RANK)
                        cout << "* all vertices are processed by Voronoi partitioning" << endl;
                    break;
                }
                double ratio = (double)active_vnum() / lastNum;
                if (_my_rank == MASTER_RANK)
                    cout << "* " << active_vnum() << " vertices (" << 100 * ((double)active_vnum() / get_vnum()) << "%) activated, #{this_round}/#{last_round}=" << 100 * ratio << "%" << endl;
                if (ratio > global_stop_ratio) {
                    if (_my_rank == MASTER_RANK)
                        cout << "-------------------------- Voronoi Partitioning END --------------------------" << endl;
                    break;
                }
                lastNum = active_vnum();
            } //reporting }

            if (active_vnum() == 0 && getBit(HAS_MSG_ORBIT, bits_bor) == 0) //all_halt AND no_msg
            {
                if (_my_rank == MASTER_RANK)
                    cout << "Inter-Round Aggregating ..." << endl;
                voronoi_compute();
                round++;
                global_step_num = 0;
                //--------------------
                global_sampling_rate *= global_factor; //increase sampling rate before voronoi_compute();
                if (_my_rank == MASTER_RANK)
                    cout << "current sampling rate = " << global_sampling_rate << endl;
                if (global_sampling_rate > global_max_rate) {
                    if (_my_rank == MASTER_RANK)
                        cout << "Sampling rate > " << global_max_rate << ", Voronoi sampling terminates" << endl;
                    active_vnum() = all_sum(active_count); //important for correctness!!!
                    break;
                }
                //--------------------
                if (_my_rank == MASTER_RANK)
                    cout << "==================== Round " << round << " ====================" << endl;
                continue;
            } else {
                clearBits();
                active_compute();
                message_buffer->sync_messages();
            }
            //===================
            worker_barrier();
            StopTimer(4);
            if (_my_rank == MASTER_RANK)
                cout << "Superstep " << global_step_num << " of Round " << round << " done. Time elapsed: " << get_timer(4) << " seconds" << endl;
        }

        unsetCombiner(); //stop Voronoi sampling

        //=========================================================

        if (active_vnum() > 0)
            subGHashMin();

        //=========================================================

        if (_my_rank == MASTER_RANK)
            cout << "============ GREEDY blk2slv ASSIGNMENT ============" << endl;
        ResetTimer(4);
        greedy_assign();
        StopTimer(4);
        if (_my_rank == MASTER_RANK) {
            cout << "============ GREEDY blk2slv DONE ============" << endl;
            cout << "Time elapsed: " << get_timer(4) << " seconds" << endl;
        }

        //=========================================================

        nbInfoExchange();

        //=========================================================

        if (_my_rank == MASTER_RANK)
            cout << "SYNC VERTICES ......" << endl;
        ResetTimer(4);
        block_sync();
        StopTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << "SYNC done in " << get_timer(4) << " seconds" << endl;

        //=========================================================

        worker_barrier();
        StopTimer(WORKER_TIMER);
        PrintTimer("Communication Time", COMMUNICATION_TIMER);
        PrintTimer("- Serialization Time", SERIALIZATION_TIMER);
        PrintTimer("- Transfer Time", TRANSFER_TIMER);
        PrintTimer("Total Computational Time", WORKER_TIMER);
        // dump graph
        ResetTimer(WORKER_TIMER);
        char* fpath = new char[params.output_path.length() + 10];
        strcpy(fpath, params.output_path.c_str());
        strcat(fpath, "/part_");
        char buffer[5];
        sprintf(buffer, "%d", _my_rank);
        strcat(fpath, buffer);
        dump_partition(fpath);
        delete fpath;
        worker_barrier();
        StopTimer(WORKER_TIMER);
        PrintTimer("Dump Time", WORKER_TIMER);
    }
};

#endif
