#ifndef BWORKER_H_
#define BWORKER_H_

#include <vector>
#include "basic/Worker.h" //for WorkerParams
#include "utils/communication.h"
#include "utils/io.h"
#include "utils/Combiner.h"
#include "VMessageBuffer.h"
#include "BMessageBuffer.h"
#include "BAggregator.h"
#include "BGlobal.h"
#include <string>
using namespace std;

//ASSUMPTIONS:
//* each worker reads "part_workerID" from HDFS
//* vertices in "part_workerID" are already grouped by blockID
template <class BlockT, class AggregatorT = BDummyAgg> //user-defined VertexT
class BWorker {
public:
    typedef typename BlockT::VertexType VertexT;
    //---------------------------------
    typedef typename VertexT::KeyType KeyT;
    typedef typename VertexT::MessageType MessageT;
    typedef typename AggregatorT::PartialType PartialT;
    typedef typename AggregatorT::FinalType FinalT;
    typedef VMessageBuffer<VertexT> VMessageBufT;
    typedef typename VMessageBufT::MessageContainerT MessageContainerT;
    //---------------------------------
    typedef typename BlockT::BMsgType BMsgT;
    typedef BMessageBuffer<BlockT> BMessageBufT;
    typedef typename BMessageBufT::MessageContainerT BMessageContainerT;
    //---------------------------------
    typedef vector<VertexT*> VertexContainer;
    typedef typename VertexContainer::iterator VertexIter;
    typedef vector<BlockT*> BlockContainer;
    typedef typename BlockContainer::iterator BlockIter;

    //===================================

    VertexContainer vertexes;
    int active_vcount;
    VMessageBufT* vmessage_buffer;
    //---------------------
    BlockContainer blocks;
    int active_bcount;
    BMessageBufT* bmessage_buffer;
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

    enum COMPUTEMODE {
        B_COMP = 0,
        V_COMP = 1,
        VB_COMP = 2
    };
    int compute_mode;
    void set_compute_mode(int mode)
    {
        compute_mode = mode;
    }
    //---------------------
    Combiner<MessageT>* combiner;
    Combiner<BMsgT>* bcombiner;
    AggregatorT* aggregator;

    //===================================

    BWorker()
    {
        //init_workers();//put to run.cpp
        vmessage_buffer = new VMessageBufT;
        global_message_buffer = vmessage_buffer;
        active_vcount = 0;
        bmessage_buffer = new BMessageBufT;
        global_bmessage_buffer = bmessage_buffer;
        active_bcount = 0;
        ///////////////
        dump_mode = B_DUMP;
        compute_mode = B_COMP;
        ///////////////
        combiner = NULL;
        global_combiner = NULL;
        bcombiner = NULL;
        global_bcombiner = NULL;
        ///////////////
        aggregator = NULL;
        global_aggregator = NULL;
        global_agg = NULL;
    }

    virtual ~BWorker()
    {
        for (VertexIter it = vertexes.begin(); it != vertexes.end(); it++)
            delete *it;
        for (BlockIter it = blocks.begin(); it != blocks.end(); it++)
            delete *it;
        delete vmessage_buffer;
        delete bmessage_buffer;
        if (getAgg() != NULL)
            delete (FinalT*)global_agg;
        //worker_finalize();//put to run.cpp
        worker_barrier(); //newly added for ease of multi-job programming in run.cpp
    }

    int getVNum()
    {
        return vertexes.size();
    }

    int getBNum()
    {
        return blocks.size();
    }

    void setCombiner(Combiner<MessageT>* cb)
    {
        combiner = cb;
        global_combiner = cb;
    }

    void setBCombiner(Combiner<BMsgT>* cb)
    {
        bcombiner = cb;
        global_bcombiner = cb;
    }

    void setAggregator(AggregatorT* ag)
    {
        aggregator = ag;
        global_aggregator = ag;
        global_agg = new FinalT;
    }

    void active_vcompute()
    {
        active_vcount = 0;
        VMessageBufT* mbuf = (VMessageBufT*)get_message_buffer();
        vector<MessageContainerT>& v_msgbufs = mbuf->get_v_msg_bufs();
        for (BlockIter it = blocks.begin(); it != blocks.end(); it++) {
            BlockT* block = *it;
            for (int i = block->begin; i < block->begin + block->size; i++) {
                if (v_msgbufs[i].size() == 0) {
                    if (vertexes[i]->is_active()) {
                        block->activate(); //vertex activates its block
                        vertexes[i]->compute(v_msgbufs[i]);
                        AggregatorT* agg = (AggregatorT*)get_aggregator();
                        if (agg != NULL)
                            agg->stepPartialV(vertexes[i]);
                        if (vertexes[i]->is_active())
                            active_vcount++;
                    }
                } else {
                    block->activate(); //vertex activates its block
                    vertexes[i]->activate();
                    vertexes[i]->compute(v_msgbufs[i]);
                    v_msgbufs[i].clear(); //clear used msgs
                    AggregatorT* agg = (AggregatorT*)get_aggregator();
                    if (agg != NULL)
                        agg->stepPartialV(vertexes[i]);
                    if (vertexes[i]->is_active())
                        active_vcount++;
                }
            }
        }
    }

    void all_vcompute()
    {
        active_vcount = 0;
        VMessageBufT* mbuf = (VMessageBufT*)get_message_buffer();
        vector<MessageContainerT>& v_msgbufs = mbuf->get_v_msg_bufs();
        for (BlockIter it = blocks.begin(); it != blocks.end(); it++) {
            BlockT* block = *it;
            block->activate(); //vertex activates its block
            for (int i = block->begin; i < block->begin + block->size; i++) {
                vertexes[i]->activate();
                vertexes[i]->compute(v_msgbufs[i]);
                v_msgbufs[i].clear(); //clear used msgs
                AggregatorT* agg = (AggregatorT*)get_aggregator();
                if (agg != NULL)
                    agg->stepPartialV(vertexes[i]);
                if (vertexes[i]->is_active())
                    active_vcount++;
            }
        }
    }

    void active_bcompute()
    {
        active_bcount = 0;
        BMessageBufT* mbuf = (BMessageBufT*)get_bmessage_buffer();
        vector<BMessageContainerT>& b_msgbufs = mbuf->get_b_msg_bufs();
        for (int i = 0; i < blocks.size(); i++) {
            if (b_msgbufs[i].size() == 0) {
                if (blocks[i]->is_active()) {
                    blocks[i]->compute(b_msgbufs[i], vertexes);
                    AggregatorT* agg = (AggregatorT*)get_aggregator();
                    if (agg != NULL)
                        agg->stepPartialB(blocks[i]);
                    if (blocks[i]->is_active())
                        active_bcount++;
                }
            } else {
                blocks[i]->activate();
                blocks[i]->compute(b_msgbufs[i], vertexes);
                b_msgbufs[i].clear(); //clear used msgs
                AggregatorT* agg = (AggregatorT*)get_aggregator();
                if (agg != NULL)
                    agg->stepPartialB(blocks[i]);
                if (blocks[i]->is_active())
                    active_bcount++;
            }
        }
    }

    void all_bcompute()
    {
        active_bcount = 0;
        BMessageBufT* mbuf = (BMessageBufT*)get_bmessage_buffer();
        vector<BMessageContainerT>& b_msgbufs = mbuf->get_b_msg_bufs();
        for (int i = 0; i < blocks.size(); i++) {
            blocks[i]->activate();
            blocks[i]->compute(b_msgbufs[i], vertexes);
            b_msgbufs[i].clear(); //clear used msgs
            AggregatorT* agg = (AggregatorT*)get_aggregator();
            if (agg != NULL)
                agg->stepPartialB(blocks[i]);
            if (blocks[i]->is_active())
                active_bcount++;
        }
    }

    //user-defined graphLoader ==============================
    virtual VertexT* toVertex(char* line) = 0; //this is what user specifies!!!!!!

    void load_vertex(VertexT* v)
    { //called by load_graph
        vertexes.push_back(v);
        if (v->is_active())
            active_vcount++;
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
    virtual void toline(BlockT* b, VertexT* v, BufferedWriter& writer) = 0; //this is what user specifies!!!!!!
    void vdump(const char* outpath)
    {
        hdfsFS fs = getHdfsFS();
        BufferedWriter* writer = new BufferedWriter(outpath, fs, _my_rank);

        for (BlockIter it = blocks.begin(); it != blocks.end(); it++) {
            BlockT* block = *it;
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
            BlockT* block = *it;
            for (int i = block->begin; i < block->begin + block->size; i++) {
                writer->check();
                toline(block, vertexes[i], *writer);
            }
        }
        delete writer;
        hdfsDisconnect(fs);
    }

    //=======================================================

    virtual void blockInit(VertexContainer& vertexList, BlockContainer& blockList) = 0; //worker.compute()

    //=======================================================

    void agg_sync()
    {
        AggregatorT* agg = (AggregatorT*)get_aggregator();
        if (agg != NULL) {
            if (_my_rank != MASTER_RANK) { //send partialT to aggregator
                //%%%%%% gathering PartialT
                PartialT* part = agg->finishPartial();
                slaveGather(*part);
                //%%%%%% scattering FinalT
                slaveBcast(*((FinalT*)global_agg));
            } else {
                //%%%%%% gathering PartialT
                vector<PartialT*> parts(_num_workers);
                masterGather(parts);
                for (int i = 0; i < _num_workers; i++) {
                    if (i != MASTER_RANK) {
                        PartialT* part = parts[i];
                        agg->stepFinal(part);
                        delete part;
                    }
                }
                //%%%%%% scattering FinalT
                FinalT* final = agg->finishFinal();
                //global_agg=final; //cannot if MASTER_RANK works as a slave, as agg->finishFinal() may change
                *((FinalT*)global_agg) = *final; //deep copy
                masterBcast(*((FinalT*)global_agg));
            }
        }
    }

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
        worker_barrier(); //@@@@@@@@@@@@@
        StopTimer(WORKER_TIMER);
        PrintTimer("Load Time", WORKER_TIMER);

        //=========================================================

        //-------- create blocks ----------
        int prev = -1;
        BlockT* block = NULL;
        int pos;
        for (pos = 0; pos < vertexes.size(); pos++) {
            int bid = vertexes[pos]->bid;
            if (bid != prev) {
                if (block != NULL) {
                    block->size = pos - block->begin;
                    blocks.push_back(block);
                }
                block = new BlockT;
                prev = block->bid = bid;
                block->begin = pos;
            }
        }
        //flush
        if (block != NULL) {
            block->size = pos - block->begin;
            blocks.push_back(block);
        }
        active_bcount = getBNum(); //initially, all blocks are active

        //we do not allow adding vertices/blocks for current version
        get_bnum() = all_sum(getBNum());
        get_vnum() = all_sum(getVNum());
        if (_my_rank == MASTER_RANK)
            cout << "* #{blocks} = " << get_bnum() << ", #{vertices} = " << get_vnum() << endl;

        blockInit(vertexes, blocks); //setting user-defined block fields

        vmessage_buffer->init(vertexes);
        bmessage_buffer->init(blocks);
        //=========================================================

        worker_barrier(); //@@@@@@@@@@@@@
        init_timers();
        ResetTimer(WORKER_TIMER);
        //supersteps
        global_step_num = 0;
        long long step_vmsg_num;
        long long step_bmsg_num;
        long long global_vmsg_num = 0;
        long long global_bmsg_num = 0;
        if (compute_mode == VB_COMP) {
            while (true) {
                global_step_num++;
                ResetTimer(4);
                //===================
                char bits_bor = all_bor(global_bor_bitmap);
                if (getBit(FORCE_TERMINATE_ORBIT, bits_bor) == 1)
                    break;
                //get_vnum()=all_sum(getVNum()); //we do not allow adding vertices/blocks for current version
                int wakeAll = getBit(WAKE_ALL_ORBIT, bits_bor);
                if (wakeAll == 0) {
                    active_vnum() = all_sum(active_vcount);
                    active_bnum() = all_sum(active_bcount);
                    if (active_vnum() == 0 && active_bnum() == 0 && getBit(HAS_MSG_ORBIT, bits_bor) == 0)
                        break; //all_halt AND no_msg
                } else {
                    active_vnum() = get_vnum();
                    active_bnum() = get_bnum();
                }
                //===================
                AggregatorT* agg = (AggregatorT*)get_aggregator();
                if (agg != NULL)
                    agg->init();
                //===================
                clearBits();
                if (wakeAll == 1) {
                    all_vcompute();
                    all_bcompute();
                } else {
                    active_vcompute();
                    active_bcompute();
                }

                vmessage_buffer->combine();
                bmessage_buffer->combine();

                step_vmsg_num = master_sum_LL(vmessage_buffer->get_total_msg());
                step_bmsg_num = master_sum_LL(bmessage_buffer->get_total_msg());
                if (_my_rank == MASTER_RANK) {
                    global_vmsg_num += step_vmsg_num;
                    global_bmsg_num += step_bmsg_num;
                }
                vmessage_buffer->sync_messages();
                bmessage_buffer->sync_messages();
                agg_sync();
                //===================
                worker_barrier();
                StopTimer(4);
                if (_my_rank == MASTER_RANK) {
                    cout << "Superstep " << global_step_num << " done. Time elapsed: " << get_timer(4) << " seconds" << endl;
                    cout << "#vmsgs: " << step_vmsg_num << ", #bmsgs: " << step_bmsg_num << endl;
                }
            }
        } else if (compute_mode == B_COMP) {
            while (true) {

                global_step_num++;
                ResetTimer(4);
                //===================
                char bits_bor = all_bor(global_bor_bitmap);
                if (getBit(FORCE_TERMINATE_ORBIT, bits_bor) == 1)
                    break;
                //get_vnum()=all_sum(getVNum()); //we do not allow adding vertices/blocks for current version
                int wakeAll = getBit(WAKE_ALL_ORBIT, bits_bor);
                if (wakeAll == 0) {
                    active_bnum() = all_sum(active_bcount);
                    if (active_bnum() == 0 && getBit(HAS_MSG_ORBIT, bits_bor) == 0)
                        break; //all_halt AND no_msg
                } else
                    active_bnum() = get_bnum();
                //===================
                AggregatorT* agg = (AggregatorT*)get_aggregator();
                if (agg != NULL)
                    agg->init();
                //===================
                clearBits();
                if (wakeAll == 1)
                    all_bcompute();
                else
                    active_bcompute();

                bmessage_buffer->combine();
                step_bmsg_num = master_sum_LL(bmessage_buffer->get_total_msg());
                if (_my_rank == MASTER_RANK) {
                    global_bmsg_num += step_bmsg_num;
                }
                bmessage_buffer->sync_messages();
                agg_sync();
                //===================
                worker_barrier();
                StopTimer(4);
                if (_my_rank == MASTER_RANK) {
                    cout << "Superstep " << global_step_num << " done. Time elapsed: " << get_timer(4) << " seconds" << endl;
                    cout << "#bmsgs: " << step_bmsg_num << endl;
                }
            }
        } else // compute_mode==V_COMP
        {
            while (true) {
                global_step_num++;
                ResetTimer(4);
                //===================
                char bits_bor = all_bor(global_bor_bitmap);
                if (getBit(FORCE_TERMINATE_ORBIT, bits_bor) == 1)
                    break;
                //get_vnum()=all_sum(getVNum()); //we do not allow adding vertices/blocks for current version
                int wakeAll = getBit(WAKE_ALL_ORBIT, bits_bor);
                if (wakeAll == 0) {
                    active_vnum() = all_sum(active_vcount);
                    if (active_vnum() == 0 && getBit(HAS_MSG_ORBIT, bits_bor) == 0)
                        break; //all_halt AND no_msg
                } else
                    active_vnum() = get_vnum();
                //===================
                AggregatorT* agg = (AggregatorT*)get_aggregator();
                if (agg != NULL)
                    agg->init();
                //===================
                clearBits();
                if (wakeAll == 1)
                    all_vcompute();
                else
                    active_vcompute();
                vmessage_buffer->combine();
                step_vmsg_num = master_sum_LL(vmessage_buffer->get_total_msg());
                if (_my_rank == MASTER_RANK) {
                    global_vmsg_num += step_vmsg_num;
                }
                vmessage_buffer->sync_messages();
                agg_sync();
                //===================
                worker_barrier();
                StopTimer(4);
                if (_my_rank == MASTER_RANK) {
                    cout << "Superstep " << global_step_num << " done. Time elapsed: " << get_timer(4) << " seconds" << endl;
                    cout << "#vmsgs: " << step_vmsg_num << endl;
                }
            }
        }
        worker_barrier();
        StopTimer(WORKER_TIMER);
        PrintTimer("Communication Time", COMMUNICATION_TIMER);
        PrintTimer("- Serialization Time", SERIALIZATION_TIMER);
        PrintTimer("- Transfer Time", TRANSFER_TIMER);
        PrintTimer("Total Computational Time", WORKER_TIMER);
        if (_my_rank == MASTER_RANK) {
            cout << "Total #msgs=" << global_vmsg_num << endl;
            cout << "Total #bmsgs=" << global_bmsg_num << endl;
        }

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

    void run(const WorkerParams& params, int num_phases)
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
        worker_barrier(); //@@@@@@@@@@@@@
        StopTimer(WORKER_TIMER);
        PrintTimer("Load Time", WORKER_TIMER);

        //=========================================================

        //-------- create blocks ----------
        int prev = -1;
        BlockT* block = NULL;
        int pos;
        for (pos = 0; pos < vertexes.size(); pos++) {
            int bid = vertexes[pos]->bid;
            if (bid != prev) {
                if (block != NULL) {
                    block->size = pos - block->begin;
                    blocks.push_back(block);
                }
                block = new BlockT;
                prev = block->bid = bid;
                block->begin = pos;
            }
        }
        //flush
        if (block != NULL) {
            block->size = pos - block->begin;
            blocks.push_back(block);
        }
        active_bcount = getBNum(); //initially, all blocks are active

        //we do not allow adding vertices/blocks for current version
        get_bnum() = all_sum(getBNum());
        get_vnum() = all_sum(getVNum());
        if (_my_rank == MASTER_RANK)
            cout << "* #{blocks} = " << get_bnum() << ", #{vertices} = " << get_vnum() << endl;

        blockInit(vertexes, blocks); //setting user-defined block fields

        vmessage_buffer->init(vertexes);
        bmessage_buffer->init(blocks);
        //=========================================================

        worker_barrier(); //@@@@@@@@@@@@@
        init_timers();
        ResetTimer(WORKER_TIMER);
        long long global_vmsg_num = 0;
        long long global_bmsg_num = 0;
        bool terminate = false;
        for (global_phase_num = 1; global_phase_num <= num_phases; global_phase_num++) {
            if (_my_rank == MASTER_RANK)
                cout << "################ Phase " << global_phase_num << " ################" << endl;
            //supersteps
            global_step_num = 0;
            long long step_vmsg_num;
            long long step_bmsg_num;

            if (compute_mode == VB_COMP) {
                while (true) {

                    global_step_num++;
                    ResetTimer(4);
                    //===================
                    char bits_bor = all_bor(global_bor_bitmap);

                    if (getBit(FORCE_TERMINATE_ORBIT, bits_bor) == 1) {
                        terminate = true;
                        break;
                    }
                    //get_vnum()=all_sum(getVNum()); //we do not allow adding vertices/blocks for current version
                    int wakeAll = getBit(WAKE_ALL_ORBIT, bits_bor);
                    if (wakeAll == 0) {
                        if (phase_num() > 1 && step_num() == 1) {
                            active_vnum() = vertexes.size();
                            active_bnum() = blocks.size();
                        } else {
                            active_vnum() = all_sum(active_vcount);
                            active_bnum() = all_sum(active_bcount);
                        }
                        if (active_vnum() == 0 && active_bnum() == 0 && getBit(HAS_MSG_ORBIT, bits_bor) == 0) {
                            break; //all_halt AND no_msg
                        }
                    } else {
                        active_vnum() = get_vnum();
                        active_bnum() = get_bnum();
                    }
                    //===================
                    AggregatorT* agg = (AggregatorT*)get_aggregator();
                    if (agg != NULL)
                        agg->init();
                    //===================
                    clearBits();

                    if (wakeAll == 1) {
                        all_vcompute();
                        all_bcompute();
                    } else if (phase_num() > 1 && step_num() == 1) {
                        all_vcompute();
                        all_bcompute();
                    } else {
                        active_vcompute();
                        active_bcompute();
                    }

                    vmessage_buffer->combine();
                    bmessage_buffer->combine();

                    step_vmsg_num = master_sum_LL(vmessage_buffer->get_total_msg());
                    step_bmsg_num = master_sum_LL(bmessage_buffer->get_total_msg());
                    if (_my_rank == MASTER_RANK) {
                        global_vmsg_num += step_vmsg_num;
                        global_bmsg_num += step_bmsg_num;
                    }
                    vmessage_buffer->sync_messages();
                    bmessage_buffer->sync_messages();
                    agg_sync();
                    //===================
                    worker_barrier();
                    StopTimer(4);
                    if (_my_rank == MASTER_RANK) {
                        cout << "Superstep " << global_step_num << " done. Time elapsed: " << get_timer(4) << " seconds" << endl;
                        cout << "#vmsgs: " << step_vmsg_num << ", #bmsgs: " << step_bmsg_num << endl;
                    }
                }
            } else if (compute_mode == B_COMP) {
                while (true) {

                    global_step_num++;
                    ResetTimer(4);
                    //===================
                    char bits_bor = all_bor(global_bor_bitmap);
                    if (getBit(FORCE_TERMINATE_ORBIT, bits_bor) == 1) {
                        terminate = true;
                        break;
                    }
                    //get_vnum()=all_sum(getVNum()); //we do not allow adding vertices/blocks for current version
                    int wakeAll = getBit(WAKE_ALL_ORBIT, bits_bor);
                    if (wakeAll == 0) {
                        if (phase_num() > 1 && step_num() == 1) {
                            active_bnum() = blocks.size();
                        } else {
                            active_bnum() = all_sum(active_bcount);
                        }

                        active_bnum() = all_sum(active_bcount);
                        if (active_bnum() == 0 && getBit(HAS_MSG_ORBIT, bits_bor) == 0)
                            break; //all_halt AND no_msg
                    } else
                        active_bnum() = get_bnum();
                    //===================
                    AggregatorT* agg = (AggregatorT*)get_aggregator();
                    if (agg != NULL)
                        agg->init();
                    //===================
                    clearBits();
                    if (wakeAll == 1)
                        all_bcompute();
                    else if (phase_num() > 1 && step_num() == 1) {
                        all_bcompute();
                    } else
                        active_bcompute();

                    bmessage_buffer->combine();
                    step_bmsg_num = master_sum_LL(bmessage_buffer->get_total_msg());
                    if (_my_rank == MASTER_RANK) {
                        global_bmsg_num += step_bmsg_num;
                    }
                    bmessage_buffer->sync_messages();
                    agg_sync();
                    //===================
                    worker_barrier();
                    StopTimer(4);
                    if (_my_rank == MASTER_RANK) {
                        cout << "Superstep " << global_step_num << " done. Time elapsed: " << get_timer(4) << " seconds" << endl;
                        cout << "#bmsgs: " << step_bmsg_num << endl;
                    }
                }
            } else // compute_mode==V_COMP
            {
                while (true) {
                    global_step_num++;
                    ResetTimer(4);
                    //===================
                    char bits_bor = all_bor(global_bor_bitmap);
                    if (getBit(FORCE_TERMINATE_ORBIT, bits_bor) == 1) {
                        terminate = true;
                        break;
                    }
                    //get_vnum()=all_sum(getVNum()); //we do not allow adding vertices/blocks for current version
                    int wakeAll = getBit(WAKE_ALL_ORBIT, bits_bor);
                    if (wakeAll == 0) {
                        if (phase_num() > 1 && step_num() == 1) {
                            active_vnum() = vertexes.size();

                        } else {
                            active_vnum() = all_sum(active_vcount);
                        }
                        active_vnum() = all_sum(active_vcount);
                        if (active_vnum() == 0 && getBit(HAS_MSG_ORBIT, bits_bor) == 0)
                            break; //all_halt AND no_msg
                    } else
                        active_vnum() = get_vnum();
                    //===================
                    AggregatorT* agg = (AggregatorT*)get_aggregator();
                    if (agg != NULL)
                        agg->init();
                    //===================
                    clearBits();
                    if (wakeAll == 1)
                        all_vcompute();
                    else if (phase_num() > 1 && step_num() == 1) {
                        all_vcompute();

                    } else
                        active_vcompute();
                    vmessage_buffer->combine();
                    step_vmsg_num = master_sum_LL(vmessage_buffer->get_total_msg());
                    if (_my_rank == MASTER_RANK) {
                        global_vmsg_num += step_vmsg_num;
                    }
                    vmessage_buffer->sync_messages();
                    agg_sync();
                    //===================
                    worker_barrier();
                    StopTimer(4);
                    if (_my_rank == MASTER_RANK) {
                        cout << "Superstep " << global_step_num << " done. Time elapsed: " << get_timer(4) << " seconds" << endl;
                        cout << "#vmsgs: " << step_vmsg_num << endl;
                    }
                }
            }
            if (terminate)
                break;
        }

        worker_barrier();
        StopTimer(WORKER_TIMER);
        PrintTimer("Communication Time", COMMUNICATION_TIMER);
        PrintTimer("- Serialization Time", SERIALIZATION_TIMER);
        PrintTimer("- Transfer Time", TRANSFER_TIMER);
        PrintTimer("Total Computational Time", WORKER_TIMER);
        if (_my_rank == MASTER_RANK) {
            cout << "Total #msgs=" << global_vmsg_num << endl;
            cout << "Total #bmsgs=" << global_bmsg_num << endl;
        }

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
