#ifndef BGLOBAL_H
#define BGLOBAL_H

#include "utils/global.h"

using namespace std;

//below are used by blogel

//========== Voronoi cell partitioning parameters ===========
double global_sampling_rate = 0.0001;
inline void set_sampRate(double rate)
{
    global_sampling_rate = rate;
}

int global_max_hop = INT_MAX; //maximum allowed #supersteps for a round of Voronoi partitioning
inline void set_maxHop(int hop)
{
    global_max_hop = hop;
}

int global_max_vcsize = INT_MAX; //maximum allowed size of a Voronoi cell
inline void set_maxVCSize(int sz)
{
    global_max_vcsize = sz;
}

double global_stop_ratio = 0.9; //switch to HashMin when #{this_round}/#{last_round}=98.9409% > global_stop_ratio
inline void set_stopRatio(double ratio)
{
    global_stop_ratio = ratio;
}

double global_max_rate = 0.1; //switch to HashMin when sampling_rate > global_max_rate
inline void set_maxRate(double ratio)
{
    global_max_rate = ratio;
}

double global_factor = 2; //switch to HashMin when #{this_round}/#{last_round}=98.9409% > global_stop_ratio
inline void set_factor(double factor)
{
    global_factor = factor;
}
//============================================================

void* global_bmessage_buffer = NULL;
inline void set_bmessage_buffer(void* mb)
{
    global_bmessage_buffer = mb;
}
inline void* get_bmessage_buffer()
{
    return global_bmessage_buffer;
}

int global_bnum = 0;
inline int& get_bnum()
{
    return global_bnum;
}
int global_active_bnum = 0;
inline int& active_bnum()
{
    return global_active_bnum;
}

void* global_bcombiner = NULL;
inline void set_bcombiner(void* cb)
{
    global_bcombiner = cb;
}
inline void* get_bcombiner()
{
    return global_bcombiner;
}

#endif
