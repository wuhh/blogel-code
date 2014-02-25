#ifndef BTYPE_H_
#define BTYPE_H_
#include "utils/type.h"
struct triplet
{
	VertexID vid;
	int bid;
	int wid;

	bool operator==(const triplet & o) const
	{
		return vid==o.vid;
	}

	friend ibinstream & operator<<(ibinstream & m, const triplet & idm){
		m<<idm.vid;
		m<<idm.bid;
		m<<idm.wid;
		return m;
	}
	friend obinstream & operator>>(obinstream &m, triplet & idm){
		m>>idm.vid;
		m>>idm.bid;
		m>>idm.wid;
		return m;
	}
};
#endif