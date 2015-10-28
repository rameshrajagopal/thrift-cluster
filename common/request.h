#ifndef _REQUEST_H_INCLUDED_
#define _REQUEST_H_INCLUDED_

#include <vector>
#include <request.h>

using namespace std;

class Request
{
public:
    Request(int rnum, const string & r, Response & res): 
           num(rnum), req(r), response(res) {}
    void updateResponse(int slaveId, int errnum)
    {
        response.update(slaveId, errnum);
    }
private:
    int num;
    const string & req;
    Response & response;
    std::mutex mutex_;
};


#endif /*_REQUEST_H_INCLUDED_*/
