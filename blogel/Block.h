#ifndef BLOCK_H_
#define BLOCK_H_

#include <vector>
#include "BMessageBuffer.h"
using namespace std;

template <class BValT, class BVertexT, class BMsgT> //BValT need not be serializable
class Block {
public:
    int bid;
    int begin;
    int size;

    //====================================

    typedef BValT BValType;
    typedef BMsgT BMsgType;
    typedef vector<BMsgType> MessageContainer;
    typedef typename MessageContainer::iterator MessageIter;
    typedef Block<BValT, BVertexT, BMsgT> BlockT;
    typedef vector<BVertexT*> VertexContainer;
    typedef BVertexT VertexType;

    //====================================

    virtual void compute(MessageContainer& messages, VertexContainer& vertexes) = 0;
    inline BValT& value()
    {
        return _value;
    }
    inline const BValT& value() const
    {
        return _value;
    }

    Block()
        : active(true)
    {
    }

    inline bool is_active()
    {
        return active;
    }
    inline void activate()
    {
        active = true;
    }
    inline void vote_to_halt()
    {
        active = false;
    }

    void send_message(int bid, const int wid, const BMsgT& msg)
    {
        ((BMessageBuffer<BlockT>*)get_bmessage_buffer())->addBMsg(bid, wid, msg);
    }

private:
    BValT _value;
    bool active;
};

#endif
