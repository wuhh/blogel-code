#ifndef VMESSAGEBUFFER_H
#define VMESSAGEBUFFER_H

#include <vector>
#include "utils/Combiner.h"
#include "utils/communication.h"
#include "BVecs.h"
#include "BGlobal.h"
using namespace std;

template <class BVertexT>
class VMessageBuffer {
public:
    typedef typename BVertexT::KeyType KeyT;
    typedef typename BVertexT::MessageType MessageT;
    typedef vector<MessageT> MessageContainerT;
    typedef hash_map<KeyT, int> Map; //int = position in v_msg_bufs
    typedef BVecs<KeyT, MessageT> VecsT;
    typedef typename VecsT::Vec Vec;
    typedef typename VecsT::VecGroup VecGroup;
    typedef typename Map::iterator MapIter;

    VecsT out_messages;
    Map in_messages;
    vector<MessageContainerT> v_msg_bufs;

    void init(vector<BVertexT*> vertexes)
    {
        v_msg_bufs.resize(vertexes.size());
        for (int i = 0; i < vertexes.size(); i++) {
            BVertexT* v = vertexes[i];
            in_messages[v->id] = i;
        }
    }

    void addVMsg(const KeyT& id, const int wid, const MessageT& msg)
    {
        hasMsg(); //cannot end yet even every vertex halts
        out_messages.append(id, wid, msg);
    }

    Map& get_messages()
    {
        return in_messages;
    }

    void combine()
    {
        //apply combiner
        Combiner<MessageT>* combiner = (Combiner<MessageT>*)get_combiner();
        if (combiner != NULL)
            out_messages.combine(combiner);
    }

    void sync_messages()
    {
        int np = get_num_workers();
        int me = get_worker_id();
        //exchange msgs
        all_to_all(out_messages.getBufs());
        // gather all messages
        for (int i = 0; i < np; i++) {
            Vec& msgBuf = out_messages.getBuf(i);
            for (int i = 0; i < msgBuf.size(); i++) {
                MapIter it = in_messages.find(msgBuf[i].key);
                if (it != in_messages.end()) //filter out msgs to non-existent vertices
                    v_msg_bufs[it->second].push_back(msgBuf[i].msg); //CHANGED FOR VADD
            }
        }
        //clear out-msg-buf
        out_messages.clear();
    }

    long long get_total_msg()
    {
        return out_messages.get_total_msg();
    }

    vector<MessageContainerT>& get_v_msg_bufs()
    {
        return v_msg_bufs;
    }
};

#endif
