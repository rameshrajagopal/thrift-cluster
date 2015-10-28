#ifndef _RESPONSE_H_INCLUDED_
#define _RESPONSE_H_INCLUDED_

#include <vector>

struct ErrorInfo
{
    int slaveId;
    int errno;
};

class Response 
{
public:
    enum Status { IN_PROGRESS, DONE };
    Response(int nReply) : numReplys(nReply), numErrors{0}, status{IN_PROGRESS}
    {}
    void update(int slaveId, int errnum)
    {
        bool notify = false;
        std::unique_lock<std::mutex> mlock(mutex_);
        if (errnum == 0) {
            --numReplys;
            if (numReplys == 0) {
                status = DONE;
                notify = true;
            }
        } else {
            ++numErrors;
            struct ErrorInfo info;
            errorInfo.push_back(info);/*could be bottleneck */
        }
        mlock.unlock();
        if (notify) cond_.notify_all();
    }
    void wait() 
    {
        std::unique_lock<std::mutex> mlock(mutex_);
        while (status == IN_PROGRESS) {
            cond_.wait(mlock);
        }
        mlock.unlock();
    }
private:
    Status status;
    int numReplys;
    int numErrors;
    std::mutex mutex_;
    std::condition_variable cond_;
    std::vector<struct ErrorInfo> errorInfo;
};


#endif /*_RESPONSE_H_INCLUDED_*/
