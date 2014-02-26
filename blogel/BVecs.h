#ifndef BVECS_H_
#define BVECS_H_

#include "utils/vecs.h" //for msgpair
using namespace std;

template <class KeyT, class MessageT>
class BVecs {
public:
    typedef vector<msgpair<KeyT, MessageT> > Vec;
    typedef vector<Vec> VecGroup;

    int np;
    VecGroup vecs;

    BVecs()
    {
        int np = _num_workers;
        this->np = np;
        vecs.resize(np);
    }

    void append(const KeyT key, const int wid, const MessageT msg)
    {
        msgpair<KeyT, MessageT> item(key, msg);
        vecs[wid].push_back(item);
    }

    Vec& getBuf(int pos)
    {
        return vecs[pos];
    }

    VecGroup& getBufs()
    {
        return vecs;
    }

    void clear()
    {
        for (int i = 0; i < np; i++) {
            vecs[i].clear();
        }
    }

    //============================
    //apply combiner logic

    void combine()
    {
        Combiner<MessageT>* combiner = (Combiner<MessageT>*)get_combiner();
        for (int i = 0; i < np; i++) {
            sort(vecs[i].begin(), vecs[i].end());
            Vec newVec;
            int size = vecs[i].size();
            if (size > 0) {
                newVec.push_back(vecs[i][0]);
                KeyT preKey = vecs[i][0].key;
                for (int j = 1; j < size; j++) {
                    msgpair<KeyT, MessageT>& cur = vecs[i][j];
                    if (cur.key != preKey) {
                        newVec.push_back(cur);
                        preKey = cur.key;
                    } else {
                        combiner->combine(newVec.back().msg, cur.msg);
                    }
                }
            }
            newVec.swap(vecs[i]);
        }
    }

    long long get_total_msg()
    {
        long long sum = 0;
        for (int i = 0; i < vecs.size(); i++) {
            sum += vecs[i].size();
        }
        return sum;
    }
};

#endif
