#ifndef BMESSAGEBUFFER_H
#define BMESSAGEBUFFER_H

#include <vector>

#include "utils/Combiner.h"
#include "utils/communication.h"
#include "BVecs.h"
#include "BGlobal.h"
using namespace std;

template<class BlockT>
class BMessageBuffer{
	public:
		typedef typename BlockT::BMsgType MessageT;
		typedef vector<MessageT> MessageContainerT;
		typedef hash_map<int, int> BHashMap;//val = position in b_msg_bufs
		typedef typename BHashMap::iterator BMapIter;
		typedef BVecs<int, MessageT> VecsT;
		typedef typename VecsT::Vec Vec;
		typedef typename VecsT::VecGroup VecGroup;

		VecsT out_messages;
		BHashMap in_messages;
		vector<MessageContainerT> b_msg_bufs;

		void init(vector<BlockT *> blocks)
		{
			b_msg_bufs.resize(blocks.size());
			for(int i=0; i<blocks.size(); i++)
			{
				BlockT * b=blocks[i];
				in_messages[b->bid]=i;
			}
		}

		void addBMsg(const int bid, const int wid, const MessageT & msg){
			hasMsg();//cannot end yet even every vertex halts
			out_messages.append(bid, wid, msg);
		}

		BHashMap & get_messages(){
			return in_messages;
		}

		void combine()
		{
			//apply combiner
			Combiner<MessageT> * combiner=(Combiner<MessageT> *)get_bcombiner();
			if(combiner!=NULL) out_messages.combine();
		}

		void sync_messages(){
			int np=get_num_workers();
			int me=get_worker_id();
			//exchange msgs
			all_to_all(out_messages.getBufs());
			// gather all messages
			for(int i=0;i<np;i++){
				Vec & msgBuf=out_messages.getBuf(i);
				for(int i=0; i<msgBuf.size(); i++)
				{
					BMapIter it=in_messages.find(msgBuf[i].key);
					if(it!=in_messages.end())//filter out msgs to non-existent vertices
						b_msg_bufs[it->second].push_back(msgBuf[i].msg); //CHANGED FOR VADD
				}
			}
			//clear out-msg-buf
			out_messages.clear();
		}

		long long get_total_msg()
		{
			return out_messages.get_total_msg();
		}

		vector<MessageContainerT> & get_b_msg_bufs()
		{
			return b_msg_bufs;
		}
};

#endif
