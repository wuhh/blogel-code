#include "utils/communication.h"
#include "blogel/BVertex.h"
#include "blogel/Block.h"
#include "blogel/BType.h"
#include "blogel/BWorker.h"
#include "blogel/BGlobal.h"

#include <iostream>
#include "math.h"
using namespace std;

#define EPS 0.01

struct PRValue {
    double pr;
    vector<triplet> edges;
    int split;
};

//------------------------------------

class PRVertex : public BVertex<VertexID, PRValue, char> {
public:
    virtual void compute(MessageContainer& messages)
    {
    } //not used
};

//====================================

struct doublepair {
    bool converge;
    double accum; //pr sum for nodes with out-degree 0
};
ibinstream& operator<<(ibinstream& m, const doublepair& v)
{
    m << v.converge;
    m << v.accum;
    return m;
}

obinstream& operator>>(obinstream& m, doublepair& v)
{
    m >> v.converge;
    m >> v.accum;
    return m;
}

//====================================

struct tuple {
    int block;
    double weight;
    int worker;
};

ibinstream& operator<<(ibinstream& m, const tuple& v)
{
    m << v.block;
    m << v.weight;
    m << v.worker;
    return m;
}

obinstream& operator>>(obinstream& m, tuple& v)
{
    m >> v.block;
    m >> v.weight;
    m >> v.worker;
    return m;
}

//------------------------------------

struct PRBlockValue {
    double pr;
    double delta;
    vector<tuple> edges;
};

//------------------------------------

class PRBlock : public Block<PRBlockValue, PRVertex, double> //msg = weight * pr
                {
public:
    virtual void compute(MessageContainer& messages, VertexContainer& vertexes)
    {
        if (step_num() == 1) {
            value().pr = 1.0 / get_bnum();
            value().delta = EPS / get_bnum() + 1; //init delta>EPS/|B|
        } else {
            double sum = 0;
            for (MessageIter it = messages.begin(); it != messages.end(); it++) {
                sum += *it;
            }
            doublepair* agg = ((doublepair*)getAgg());
            if (agg->converge) {
                vote_to_halt();
                return;
            }
            double residual = agg->accum / get_bnum();
            double newVal = 0.15 / get_bnum() + 0.85 * (sum + residual);
            value().delta = fabs(newVal - value().pr);
            value().pr = newVal;
        }
        for (vector<tuple>::iterator it = value().edges.begin(); it != value().edges.end(); it++) {
            send_message(it->block, it->worker, it->weight * value().pr);
        }
    }
};

//====================================

class PRSum : public BAggregator<PRVertex, PRBlock, doublepair, doublepair> {
private:
    doublepair pair;

public:
    virtual void init()
    {
        pair.converge = true;
        pair.accum = 0;
    }

    virtual void stepPartialV(PRVertex* v) {}; //not used

    virtual void stepPartialB(PRBlock* b)
    {
        if (b->value().edges.size() == 0)
            pair.accum += b->value().pr;
        if (b->value().delta > EPS / get_bnum())
            pair.converge = false;
    }

    virtual void stepFinal(doublepair* part)
    {
        pair.accum += part->accum;
        if (part->converge == false)
            pair.converge = false;
    }

    virtual doublepair* finishPartial()
    {
        return &pair;
    }

    virtual doublepair* finishFinal()
    {
        return &pair;
    }
};

//====================================

class PRWorker : public BWorker<PRBlock, PRSum> {

public:
    void localPR(PRBlock* block, VertexContainer& vertexes)
    {
        bool converge = false;
        double accum = 0;
        int num = block->size;
        double* pr_buf;
        int round = 1;
        double threshold = EPS / num;
        while (converge == false) {
            double oldaccum;
            double* old_pr_buf;
            if (round > 1) {
                converge = true;
                oldaccum = accum;
                accum = 0;
                old_pr_buf = pr_buf;
            }
            pr_buf = new double[num];
            for (int i = 0; i < num; i++)
                pr_buf[i] = 0;
            for (int i = block->begin; i < block->begin + block->size; i++) {
                PRVertex* vertex = vertexes[i];
                if (round == 1) {
                    vertex->value().pr = 1.0 / num;
                } else {
                    int logID = i - block->begin;
                    double new_pr = 0.15 / num + 0.85 * (old_pr_buf[logID] + oldaccum / num);
                    double delta = new_pr - vertex->value().pr;
                    if (fabs(delta) > threshold)
                        converge = false;
                    vertex->value().pr = new_pr;
                }
                //------
                vector<triplet>& edges = vertex->value().edges;
                int split = vertex->value().split;
                if (split == -1)
                    accum += vertex->value().pr;
                else {
                    double msg = vertex->value().pr / (split + 1);
                    for (int j = 0; j <= split; j++) {
                        triplet nb = edges[j];
                        int phyID = nb.wid;
                        int logID = phyID - block->begin;
                        pr_buf[logID] += msg;
                    }
                }
            }
            if (round > 1)
                delete old_pr_buf;
            round++;
        }
        //	cout<<"Worker "<<_my_rank<<": Block "<<block->blockID<<" local-pr for "<<round-1<<" rounds"<<endl;//DEGUG !!!!!!
        delete pr_buf;
    }

    virtual void blockInit(VertexContainer& vertexes, BlockContainer& blocks)
    {
        ResetTimer(4);
        hash_map<int, int> map;
        for (int i = 0; i < vertexes.size(); i++)
            map[vertexes[i]->id] = i;
        //////
        if (_my_rank == MASTER_RANK)
            cout << "Splitting in/out-block edges ..." << endl;
        for (BlockIter it = blocks.begin(); it != blocks.end(); it++) {
            PRBlock* block = *it;
            for (int i = block->begin; i < block->begin + block->size; i++) {
                PRVertex* vertex = vertexes[i];
                vector<triplet>& edges = vertex->value().edges;
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
        StopTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << "In/out-block edges split. Time elapsed: " << get_timer(4) << " seconds" << endl;

        //----------------------------------------------

        ResetTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << "Local PageRank computing ..." << endl;
        for (BlockIter it = blocks.begin(); it != blocks.end(); it++) {
            PRBlock* block = *it;
            localPR(block, vertexes);
        }
        StopTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << "Local PageRank computed. Time elapsed: " << get_timer(4) << " seconds" << endl;

        //----------------------------------------------

        ResetTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << "Restoring from local-array-index to worker-id ..." << endl;
        for (BlockIter it = blocks.begin(); it != blocks.end(); it++) {
            PRBlock* block = *it;
            for (int i = block->begin; i < block->begin + block->size; i++) {
                PRVertex* vertex = vertexes[i];
                vector<triplet>& edges = vertex->value().edges;
                int split = vertex->value().split;
                for (int j = 0; j <= split; j++) {
                    edges[j].wid = _my_rank;
                }
            }
        }
        StopTimer(4);
        if (_my_rank == MASTER_RANK)
            cout << "Worker-id restored. Time elapsed: " << get_timer(4) << " seconds" << endl;

        //----------------------------------------------

        ResetTimer(4);
        cout << "Worker " << _my_rank << ": initializing block edges ..." << endl;
        for (BlockIter it = blocks.begin(); it != blocks.end(); it++) {
            PRBlock* block = *it;
            hash_map<int, tuple> bmap; //bmap[BJ]=current weight of (BI->BJ)
            for (int i = block->begin; i < block->begin + block->size; i++) {
                PRVertex* vertex = vertexes[i];
                vector<triplet>& vedges = vertex->value().edges;
                int degree = vedges.size();
                //group vertex's neighbors by blockID
                hash_map<int, tuple> count; //count[BJ]=# of neighbors belonging to BJ
                for (int j = 0; j < degree; j++) {
                    int blockID = vedges[j].bid;
                    int workerID = vedges[j].wid;
                    hash_map<int, tuple>::iterator cit = count.find(blockID);
                    if (cit == count.end()) {
                        tuple cur = { blockID, 1, workerID };
                        count[blockID] = cur;
                    } else
                        cit->second.weight++;
                }
                //accumulate to the sums
                for (hash_map<int, tuple>::iterator cit = count.begin(); cit != count.end(); cit++) {
                    int blockID = cit->first;
                    double cnt = cit->second.weight;
                    int workerID = cit->second.worker;
                    double val = vertex->value().pr * cnt / degree;
                    hash_map<int, tuple>::iterator bit = bmap.find(blockID);
                    if (bit == bmap.end()) {
                        tuple cur = { blockID, val, workerID };
                        bmap[blockID] = cur;
                    } else
                        bit->second.weight += val;
                }
            }
            //bmap -> block's adj-list
            vector<tuple>& adj_list = block->value().edges;
            double wsum = 0;
            for (hash_map<int, tuple>::iterator bit = bmap.begin(); bit != bmap.end(); bit++) {
                adj_list.push_back(bit->second);
                wsum += bit->second.weight;
            }
            //normalize weights
            for (int i = 0; i < adj_list.size(); i++)
                adj_list[i].weight /= wsum;
        }
        StopTimer(4);
        cout << "Worker " << _my_rank << ": block edges initialized. Time elapsed: " << get_timer(4) << " seconds" << endl;
    }

    //input line format: me \t nb1 nb2 ...
    //each item is of format "vertexID blockID workerID"
    virtual PRVertex* toVertex(char* line)
    {
        char* pch;
        PRVertex* v = new PRVertex;
        pch = strtok(line, " ");
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        v->bid = atoi(pch);
        pch = strtok(NULL, "\t");
        v->wid = atoi(pch);
        while (pch = strtok(NULL, " ")) {
            triplet trip;
            trip.vid = atoi(pch);
            pch = strtok(NULL, " ");
            trip.bid = atoi(pch);
            pch = strtok(NULL, " ");
            trip.wid = atoi(pch);
            v->value().edges.push_back(trip);
        }
        return v;
    }

    //append v.local-pr block(v).pr after '\t'
    virtual void toline(PRBlock* b, PRVertex* v, BufferedWriter& writer)
    {
        char buf[1024];
        int bsize = b->size;
        sprintf(buf, "%d %d %d\t%e %e %d", v->id, v->bid, v->wid, v->value().pr, b->value().pr, bsize);
        writer.write(buf);

        vector<triplet>& nbs = v->value().edges;
        for (int i = 0; i < nbs.size(); i++) {
            sprintf(buf, " %d %d %d", nbs[i].vid, nbs[i].bid, nbs[i].wid);
            writer.write(buf);
        }
        writer.write("\n");
    }
};

class PRCombiner : public Combiner<double> {
public:
    virtual void combine(double& old, const double& new_msg)
    {
        old += new_msg;
    }
};

void blogel_app_pagerank1(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    PRWorker worker;
    PRCombiner combiner;
    worker.setBCombiner(&combiner);
    PRSum agg;
    worker.setAggregator(&agg);
    worker.run(param);
}
