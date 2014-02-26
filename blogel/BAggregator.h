#ifndef BAGGREGATOR_H
#define BAGGREGATOR_H

#include <stddef.h>

template <class VertexT, class BlockT, class PartialT, class FinalT>
class BAggregator {
public:
    typedef VertexT VertexType;
    typedef PartialT PartialType;
    typedef FinalT FinalType;

    virtual void init() = 0;
    virtual void stepPartialV(VertexT* v) = 0;
    virtual void stepPartialB(BlockT* b) = 0;
    virtual void stepFinal(PartialT* part) = 0;
    virtual PartialT* finishPartial() = 0;
    virtual FinalT* finishFinal() = 0;
};

class BDummyAgg : public BAggregator<void, void, char, char> {

public:
    virtual void init()
    {
    }
    virtual void stepPartialV(void* v)
    {
    }
    virtual void stepPartialB(void* v)
    {
    }
    virtual void stepFinal(char* part)
    {
    }
    virtual char* finishPartial()
    {
        return NULL;
    }
    virtual char* finishFinal()
    {
        return NULL;
    }
};

#endif
