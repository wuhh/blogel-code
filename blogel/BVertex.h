#ifndef BVERTEX_H
#define BVERTEX_H


#include <vector>
#include "utils/serialization.h"
#include "VMessageBuffer.h"
#include "BAggregator.h"
#include "BGlobal.h"
using namespace std;

template<class KeyT, class ValueT, class MessageT>
class BVertex
{
	public:
		KeyT id;
		int bid;
		int wid;

		//=============================================

		typedef KeyT KeyType;
		typedef ValueT ValueType;
		typedef MessageT MessageType;
		typedef vector<MessageType> MessageContainer;
		typedef typename MessageContainer::iterator MessageIter;
		typedef BVertex<KeyT, ValueT, MessageT> BVertexT;
		typedef VMessageBuffer<BVertexT> MessageBufT;

		//=============================================

		friend ibinstream & operator<<(ibinstream & m, const BVertexT & v){
			m<<v.id;
			m<<v.bid;
			m<<v.wid;
			m<<v._value;
			return m;
		}

		friend obinstream & operator>>(obinstream & m, BVertexT & v){
			m>>v.id;
			m>>v.bid;
			m>>v.wid;
			m>>v._value;
			return m;
		}

		virtual void compute(MessageContainer & messages)=0;
		inline ValueT & value() {return _value;}
		inline const ValueT & value() const {return _value;}

		BVertex():active(true){}

		inline bool is_active() {return active;}
		inline void activate() {active=true;}
		inline void vote_to_halt() {active=false;}

		void send_message(const KeyT & tgt, const int wid, const MessageT & msg){
			((MessageBufT *)get_message_buffer())->addVMsg(tgt, wid, msg);
		}

	private:
		ValueT _value;
		bool active;
};

#endif
