#ifndef STRPART_H_
#define STRPART_H_

#include "utils/serialization.h"
#include "utils/global.h"
#include "utils/communication.h"
#include "utils/ydhdfs.h"
#include "utils/type.h"
#include "basic/Worker.h"
#include "basic/Vertex.h"
#include <vector>
#include <iostream>
#include <stdlib.h>
#include <ext/hash_map>

struct STRVertex
{
	VertexID id;
	int bid;
	vector<VertexID> neighbors;
	vector<triplet> nbsInfo;
	double x;
	double y;
	string content;
};

ibinstream & operator<<(ibinstream & m, const STRVertex & v){
	m<<v.id;
	m<<v.bid;
	m<<v.neighbors;
	m<<v.nbsInfo;
	m<<v.x;
	m<<v.y;
	m<<v.content;
	return m;
}

obinstream & operator>>(obinstream & m, STRVertex & v){
	m>>v.id;
	m>>v.bid;
	m>>v.neighbors;
	m>>v.nbsInfo;
	m>>v.x;
	m>>v.y;
	m>>v.content;
	return m;
}

//Round 1: super-block generation
//- sampling
//- build super-blocks(tiles) from samples
//----------------------------------------
//Round 2: BFS(each super-block)
//- after BFS, update block info

//STRWorker is for Round 1, nbinfoExchange with hashing
//Round 2 is by another job in vmode, nbinfoExchange with (bid, wid)
class STRWorker{
	typedef hash_map<int, int> HashMap;
	typedef HashMap::iterator MapIter;

	public:
		vector<STRVertex*> _my_part;
		HashMap blk2slv;
		int xnum;
		int ynum;
		double sampleRate;

		DefaultHash<VertexID> hash;

	public:
		STRWorker(int xnum, int ynum, double sampleRate)
		{
			//init_workers();//@@@@@@@@@@@@@@@@@@@
			srand((unsigned)time(NULL));
			this->xnum=xnum;
			this->ynum=ynum;
			this->sampleRate=sampleRate;
		}

		~STRWorker()
		{
			for(int i=0; i<_my_part.size(); i++) delete _my_part[i];
			//worker_finalize();//@@@@@@@@@@@@@@@@@@@
		}

		//user-defined graphLoader ==============================
		virtual STRVertex* toVertex(char* line)=0;//this is what user specifies!!!!!!

		void load_vertex(STRVertex* v){//called by load_graph
			_my_part.push_back(v);
		}

		void load_graph(const char* inpath){
			hdfsFS fs = getHdfsFS();
			hdfsFile in=getRHandle(inpath, fs);
			LineReader reader(fs, in);
			while(true)
			{
				reader.readLine();
				if(!reader.eof()) load_vertex(toVertex(reader.getLine()));
				else break;
			}
			hdfsCloseFile(fs, in);
			hdfsDisconnect(fs);
			cout<<"Worker "<<_my_rank<<": \""<<inpath<<"\" loaded"<<endl;//DEBUG !!!!!!!!!!
		}
		//=======================================================

		//user-defined graphDumper ==============================
		virtual char* toline(STRVertex* v)=0;//this is what user specifies!!!!!!

		void dump_partition(const char* outpath){//only writes to one file "part_machineID"
			hdfsFS fs = getHdfsFS();
			hdfsFile hdl=getWHandle(outpath, fs);
			for(int i=0; i<_my_part.size(); i++)
			{
				char* line=toline(_my_part[i]);
				if(line!=NULL)
				{
					int len=strlen(line);
					line[len]='\n';
					tSize numWritten=hdfsWrite(fs, hdl, line, len+1);
					if(numWritten==-1)
					{
						fprintf(stderr, "Failed to write file!\n");
						exit(-1);
					}
				}
			}
			if(hdfsFlush(fs, hdl)){
				fprintf(stderr, "Failed to 'flush' %s\n", outpath);
				exit(-1);
			}
			hdfsCloseFile(fs, hdl);
			hdfsDisconnect(fs);
		}

		//=======================================================
		struct point
		{
			double x;
			double y;

			friend ibinstream & operator<<(ibinstream & m, const point & v){
				m<<v.x;
				m<<v.y;
				return m;
			}

			friend obinstream & operator>>(obinstream &m, point & v){
				m>>v.x;
				m>>v.y;
				return m;
			}
		};

		struct x_less
		{
			bool operator()(point const & a, point const & b) const
			{
				if(a.x<b.x) return true;
				else return false;
			}
		};

		void getSamples(vector<point> & samples)//called by "getXSplits()"
		{
			for(int i=0; i<_my_part.size(); i++)
			{
				double samp=((double)rand())/RAND_MAX;
				if(samp<=sampleRate)
				{
					point p={_my_part[i]->x, _my_part[i]->y};
					samples.push_back(p);
				}
			}
		}

		//--------------------------------------------------------

		void getSplits(vector<double> & xsplit, vector<vector<double> > & ysplits)
		{
			vector<point> samples;
			getSamples(samples);
			if(_my_rank!=MASTER_RANK)
			{//send samples to master
				slaveGather(samples);
				slaveBcast(xsplit);
				slaveBcast(ysplits);
			}
			else
			{
				vector<vector<point> > parts(_num_workers);
				masterGather(parts);
				for(int i=0; i<_num_workers; i++)
				{
					if(i!=MASTER_RANK)
					{
						samples.insert(samples.end(), parts[i].begin(), parts[i].end());
					}
				}
				//sort by x-coord
				sort(samples.begin(), samples.end(), x_less());
				//get splits here
				int size=samples.size();
				int residual=size%xnum;
				int step;
				if(residual==0) step=size/xnum;
				else step=size/xnum+1;
				for(int pos=step-1; pos<size; pos+=step) xsplit.push_back(samples[pos].x);
				//------
				vector<vector<double> > subSamps(xnum);
				for(int i=0; i<xnum; i++)
				{
					int start=step*i;
					int end=start+step;
					if(end>size) end=size;
					for(int j=start; j<end; j++)
					{
						subSamps[i].push_back(samples[j].y);
					}
					sort(subSamps[i].begin(), subSamps[i].end());
					int sz=subSamps[i].size();
					int res=sz%ynum;
					int ystep;
					if(res==0) ystep=sz/ynum;
					else ystep=sz/ynum+1;
					for(int pos=ystep-1; pos<sz; pos+=ystep) ysplits[i].push_back(subSamps[i][pos]);
				}
				//scattering splits
				masterBcast(xsplit);
				masterBcast(ysplits);
			}
		}

		//=======================================================

		int getXid(double x, vector<double> & xsplit)
		{
			int size=xsplit.size();
			if(size>xnum) size=xnum;
			for(int i=0; i<size; i++)
			{
				if(x<=xsplit[i]) return i;
			}
			return xnum-1;
		}//may improve by binary search

		int getYid(double y, vector<double> & ysplit)
		{
			int size=ysplit.size();
			if(size>ynum) size=ynum;
			for(int i=0; i<size; i++)
			{
				if(y<=ysplit[i]) return i;
			}
			return ynum-1;
		}//may improve by binary search

		int getBlkID(double x, double y, vector<double> & xsplit, vector<vector<double> > & ysplits)
		{
			int xid=getXid(x, xsplit);
			int yid=getYid(y, ysplits[xid]);
			return xid*ynum+yid;
		}

		void setBlkID(vector<double> & xsplit, vector<vector<double> > & ysplits)
		{
			for(int i=0; i<_my_part.size(); i++)
			{
				STRVertex* v=_my_part[i];
				v->bid=getBlkID(v->x, v->y, xsplit, ysplits);
			}
		}

		//=======================================================

		//=======================================================
		struct sizedBlock
		{
			int blkID;
			int size;

			bool operator<(const sizedBlock& o) const
			{
				return size>o.size;//large file goes first
			}
		};

		void greedy_assign()
		{
			//collect Voronoi cell size
			HashMap map;
			for(int i=0; i<_my_part.size(); i++)
			{
				STRVertex * v=_my_part[i];
				int blkID=v->bid;
				MapIter mit=map.find(blkID);
				if(mit==map.end()) map[blkID]=1;
				else mit->second++;
			}

			//inter-round aggregating
			if(_my_rank!=MASTER_RANK)
			{//send partialT to blkMap
				//%%%%%% gathering blkMap
				slaveGather(map);
				//%%%%%% scattering blk2slv
				slaveBcast(blk2slv);
			}
			else
			{
				//%%%%%% gathering blkMap
				vector<HashMap> parts(_num_workers);
				masterGather(parts);

				for(int i=0; i<_num_workers; i++)
				{
					if(i!=MASTER_RANK)
					{
						HashMap & part=parts[i];
						for(MapIter it=part.begin(); it!=part.end(); it++)
						{
							VertexID color=it->first;
							MapIter mit=map.find(color);
							if(mit==map.end()) map[color]=it->second;
							else mit->second+=it->second;
						}
					}
				}
				//-------------------------
				vector<sizedBlock> sizedblocks;
				hash_map<int, int> histogram;//%%% for print only %%%
				for(MapIter it=map.begin(); it!=map.end(); it++)
				{
					sizedBlock cur={it->first, it->second};
					sizedblocks.push_back(cur);
					//{ %%% for print only %%%
					int key=numDigits(it->second);
					hash_map<int, int>::iterator hit=histogram.find(key);
					if(hit==histogram.end()) histogram[key]=1;
					else hit->second++;
					//%%% for print only %%% }
				}
				sort(sizedblocks.begin(), sizedblocks.end());
				int* assigned=new int[_num_workers];
				int* bcount=new int[_num_workers];//%%% for print only %%%
				for(int i=0; i<_num_workers; i++)
				{
					assigned[i]=0;
					bcount[i]=0;
				}
				for(int i=0; i<sizedblocks.size(); i++)
				{
					int min=0;
					int minSize=assigned[0];
					for(int j=1; j<_num_workers; j++)
					{
						if(minSize>assigned[j])
						{
							min=j;
							minSize=assigned[j];
						}
					}
					sizedBlock & cur=sizedblocks[i];
					blk2slv[cur.blkID]=min;
					bcount[min]++;//%%% for print only %%%
					assigned[min]+=cur.size;
				}
				//------------------- report begin -------------------
				cout<<"* block size histogram:"<<endl;
				for(hash_map<int, int>::iterator hit=histogram.begin(); hit!=histogram.end(); hit++)
				{
					cout<<"|V|<"<<hit->first<<": "<<hit->second<<" blocks"<<endl;
				}
				cout<<"* per-machine block assignment:"<<endl;
				for(int i=0; i<_num_workers; i++)
				{
					cout<<"Machine_"<<i<<" is assigned "<<bcount[i]<<" blocks, "<<assigned[i]<<" vertices"<<endl;
				}
				//------------------- report end ---------------------
				delete[] bcount;
				delete[] assigned;
				//%%%%%% scattering blk2slv
				masterBcast(blk2slv);
			}
		}

		//=======================================================

		int slaveOf(STRVertex* v)
		{
			return blk2slv[v->bid];
		}

		int numDigits(int number)
		{
			int cur=1;
			while(number!=0)
			{
				number/=10;
				cur*=10;
			}
			return cur;
		}

		//=======================================================

		struct blk_less
		{
			bool operator()(STRVertex* const & a, STRVertex* const & b) const
			{
				if(a->bid<b->bid) return true;
				else return false;
			}
		};

		void block_sync()
		{
			//ResetTimer(4);//DEBUG !!!!!!!!!!
			//set send buffer
			vector<vector<STRVertex*> > _loaded_parts(_num_workers);
			for(int i=0; i<_my_part.size(); i++)
			{
				STRVertex* v=_my_part[i];
				_loaded_parts[slaveOf(v)].push_back(v);
			}
			//exchange vertices to add
			all_to_all(_loaded_parts);
			//delete sent vertices
			for(int i=0; i<_my_part.size(); i++)
			{
				STRVertex* v=_my_part[i];
				if(slaveOf(v)!=_my_rank) delete v;
			}
			_my_part.clear();
			//collect vertices to add
			for(int i=0; i<_num_workers; i++){
				_my_part.insert(_my_part.end(), _loaded_parts[i].begin(), _loaded_parts[i].end());
			}
			//===== deprecated ======
			sort(_my_part.begin(), _my_part.end(), blk_less()); //groupby block_id
			//StopTimer(4);//DEBUG !!!!!!!!!!
			//PrintTimer("Reduce Time",4);//DEBUG !!!!!!!!!!
		}

		//=======================================================

		//=======================================================
		typedef vector<hash_map<int, triplet> > MapSet;

		struct IDTrip{//remember to free "messages" outside
			VertexID id;
			triplet trip;

			friend ibinstream & operator<<(ibinstream & m, const IDTrip & idm){
				m<<idm.id;
				m<<idm.trip;
				return m;
			}
			friend obinstream & operator>>(obinstream &m, IDTrip & idm){
				m>>idm.id;
				m>>idm.trip;
				return m;
			}
		};

		typedef vector<vector<IDTrip> > ExchangeT;

		void nbInfoExchange()
		{
			ResetTimer(4);
			if(_my_rank==MASTER_RANK) cout<<"============= Neighbor InfoExchange Phase 1 (send hash_table) ============="<<endl;
			MapSet maps(_num_workers);
			for(int i=0; i<_my_part.size(); i++)
			{
				VertexID vid=_my_part[i]->id;
				int blockID=_my_part[i]->bid;
				triplet trip={vid, blockID, blk2slv[blockID]};
				vector<VertexID> & nbs=_my_part[i]->neighbors;
				for(int i=0; i<nbs.size(); i++)
				{
					maps[hash(nbs[i])][vid]=trip;
				}
			}
			//////
			ExchangeT recvBuf(_num_workers);
			all_to_all(maps, recvBuf);
			hash_map<VertexID, triplet> & mymap=maps[_my_rank];
			// gather all table entries
			for(int i=0;i<_num_workers;i++){
				if(i!=_my_rank)
				{
					maps[i].clear();//free sent table
					/////
					vector<IDTrip> & entries=recvBuf[i];
					for(int j=0; j<entries.size(); j++)
					{
						IDTrip & idm=entries[j];
						mymap[idm.id]=idm.trip;
					}
				}
			}
			//////
			StopTimer(4);
			if(_my_rank==MASTER_RANK) cout<<get_timer(4)<<" seconds elapsed"<<endl;
			ResetTimer(4);
			if(_my_rank==MASTER_RANK) cout<<"============= Neighbor InfoExchange Phase 2 ============="<<endl;
			for(int i=0; i<_my_part.size(); i++)
			{
				STRVertex & vcur=*_my_part[i];
				vector<VertexID> & nbs=vcur.neighbors;
				vector<triplet> & infos=vcur.nbsInfo;
				for(int i=0; i<nbs.size(); i++)
				{
					VertexID nb=nbs[i];
					triplet trip=mymap[nb];
					infos.push_back(trip);
				}
			}
			StopTimer(4);
			if(_my_rank==MASTER_RANK) cout<<get_timer(4)<<" seconds elapsed"<<endl;
		}

		//=======================================================
		void sync_graph(){
			//ResetTimer(4);//DEBUG !!!!!!!!!!
			//set send buffer
			vector<vector<STRVertex*> > _loaded_parts(_num_workers);
			for(int i=0; i<_my_part.size(); i++)
			{
				STRVertex* v=_my_part[i];
				_loaded_parts[hash(v->id)].push_back(v);
			}
			//exchange vertices to add
			all_to_all(_loaded_parts);
			//delete sent vertices
			for(int i=0; i<_my_part.size(); i++)
			{
				STRVertex* v=_my_part[i];
				if(hash(v->id)!=_my_rank) delete v;
			}
			_my_part.clear();
			//collect vertices to add
			for(int i=0; i<_num_workers; i++){
				_my_part.insert(_my_part.end(), _loaded_parts[i].begin(), _loaded_parts[i].end());
			}
			//===== deprecated ======
			//_my_part.sort(); //no need to sort now
			_loaded_parts.clear();
			//StopTimer(4);//DEBUG !!!!!!!!!!
			//PrintTimer("Reduce Time",4);//DEBUG !!!!!!!!!!
		};

		//=======================================================

		// run the worker
		void run(const WorkerParams & params){
			//check path + init
			if(_my_rank==MASTER_RANK){
				if(dirCheck(params.input_path.c_str(), params.output_path.c_str(), _my_rank==MASTER_RANK, params.force_write)==-1) return;
			}
			init_timers();

			//dispatch splits
			ResetTimer(WORKER_TIMER);
			vector<vector<string> > * arrangement;
			if(_my_rank==MASTER_RANK)
			{
				arrangement=params.native_dispatcher ? dispatchLocality(params.input_path.c_str()) : dispatchRan(params.input_path.c_str());
				//reportAssignment(arrangement);//DEBUG !!!!!!!!!!
				masterScatter(*arrangement);
				vector<string> & assignedSplits=(*arrangement)[0];
				//reading assigned splits (map)
				for(vector<string>::iterator it=assignedSplits.begin();
						it!=assignedSplits.end(); it++) load_graph(it->c_str());
				delete arrangement;
			}
			else{
				vector<string> assignedSplits;
				slaveScatter(assignedSplits);
				//reading assigned splits (map)
				for(vector<string>::iterator it=assignedSplits.begin();
						it!=assignedSplits.end(); it++) load_graph(it->c_str());
			}

			//send vertices according to hash_id (reduce)
			sync_graph();

			//barrier for data loading
			//worker_barrier();
			StopTimer(WORKER_TIMER);
			PrintTimer("Load Time",WORKER_TIMER);

			//=========================================================

			init_timers();
			ResetTimer(WORKER_TIMER);

			//----------- main logic -----------
			vector<double> xsplit;
			vector<vector<double> > ysplits(xnum);
			getSplits(xsplit, ysplits);
			//{ %%% for print only %%%
			if(_my_rank==MASTER_RANK)
			{
				cout<<"xsplit = [";
				for(int i=0; i<xsplit.size(); i++) cout<<xsplit[i]<<" ";
				cout<<"]"<<endl;
				for(int i=0; i<ysplits.size(); i++)
				{
					cout<<"ysplit["<<i<<"] = [";
					for(int j=0; j<ysplits[i].size(); j++) cout<<ysplits[i][j]<<" ";
					cout<<"]"<<endl;
				}
			}
			//%%% for print only %%% }
			setBlkID(xsplit, ysplits);
			//////
			if(_my_rank==MASTER_RANK) cout<<"============ GREEDY blk2slv ASSIGNMENT ============"<<endl;
			ResetTimer(4);
			greedy_assign();
			StopTimer(4);
			if(_my_rank==MASTER_RANK){
				cout<<"============ GREEDY blk2slv DONE ============"<<endl;
				cout<<"Time elapsed: "<<get_timer(4)<<" seconds"<<endl;
			}
			//////
			nbInfoExchange();
			//////
			if(_my_rank==MASTER_RANK) cout<<"SYNC VERTICES ......"<<endl;
			ResetTimer(4);
			block_sync();
			StopTimer(4);
			if(_my_rank==MASTER_RANK) cout<<"SYNC done in "<<get_timer(4)<<" seconds"<<endl;

			//=========================================================

			StopTimer(WORKER_TIMER);
			PrintTimer("Communication Time", COMMUNICATION_TIMER);
			PrintTimer("- Serialization Time", SERIALIZATION_TIMER);
			PrintTimer("- Transfer Time", TRANSFER_TIMER);
			PrintTimer("Total Computational Time",WORKER_TIMER);

			//=========================================================

			// dump graph
			ResetTimer(WORKER_TIMER);
			char* fpath=new char[params.output_path.length()+10];
			strcpy(fpath, params.output_path.c_str());
			strcat(fpath, "/part_");
			char buffer[5];
			sprintf(buffer, "%d", _my_rank);
			strcat(fpath, buffer);
			dump_partition(fpath);
			delete fpath;
			StopTimer(WORKER_TIMER);
			PrintTimer("Dump Time",WORKER_TIMER);
		}

};

#endif
