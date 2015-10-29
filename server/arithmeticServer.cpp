// This autogenerated skeleton file illustrates how to build a server.
// You should copy it to another filename to avoid overwriting it.

#include "ArithmeticService.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/server/TThreadedServer.h>

#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/server/TNonblockingServer.h>

#include <atomic>
#include <thread>
#include <queue.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

#define MAX_NUM_THREADS (10)
class ProxySlave;
extern std::shared_ptr<ProxySlave> getProxySlave(const std::string & addr, const int port, int id);
extern void processRequest(int reqNum, const std::string & req, std::vector<std::shared_ptr<ProxySlave>> & slaves);

using boost::shared_ptr;

using namespace  ::tutorial::arithmetic::gen;

class ArithmeticServiceHandler : virtual public ArithmeticServiceIf {
 public:
  ArithmeticServiceHandler(Queue<int> & q, Queue<int> & res):
      queue(q), resQ(res)
    {}

  int64_t add(const int32_t num1, const int32_t num2) {
      queue.push(reqNum++);
      int ret = resQ.pop();
      return ret;
  }

  int64_t multiply(const int32_t num1, const int32_t num2) {
      queue.push(reqNum++);
      int ret = resQ.pop();
      return ret;
  }
 private:
  std::atomic<int> reqNum {0};
  Queue<int> & queue;
  Queue<int> & resQ;
};

void masterTask(Queue<int> & q, Queue<int> & resQ)
{
  int port = 9090;

  shared_ptr<ArithmeticServiceHandler> handler(new ArithmeticServiceHandler(q, resQ));
  shared_ptr<TProcessor> processor(new ArithmeticServiceProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
#ifdef NON_BLOCKING
  shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());
#elif THREAD_POOL
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
#else
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
#endif
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

  shared_ptr<ThreadManager> threadManager =  ThreadManager::newSimpleThreadManager(MAX_NUM_THREADS);
  shared_ptr<ThreadFactory> threadFactory(new PosixThreadFactory());
  threadManager->threadFactory(threadFactory);
  threadManager->start();

#ifdef NON_BLOCKING
  shared_ptr<TNonblockingServer> server = 
      shared_ptr<TNonblockingServer> (new TNonblockingServer(processor, 
                                                             protocolFactory, 
                                                             port,
                                                             threadManager));
#elif THREAD_POOL
  shared_ptr<TServer> server =
    shared_ptr<TServer>(new TThreadPoolServer(processor,
                                              serverTransport,
                                              transportFactory,
                                              protocolFactory,
                                              threadManager));
#else
  shared_ptr<TServer> server =
    shared_ptr<TServer>(new TThreadedServer(processor,
                                            serverTransport,
                                            transportFactory,
                                            protocolFactory));
#endif
  server->serve();
}

int ProxySlaveTask(Queue<int> & q, Queue<int> & resQ);
int main(int argc, char **argv) 
{
   Queue<int> q;
   Queue<int> resQ;
   ProxySlaveTask(q, resQ);

   std::thread th([&q, &resQ]() {
      masterTask(q, resQ);
   });
   th.detach();
#ifdef TEST
   for (int num = 0; num < 10; ++num) {
        q.push(num);
        usleep(100 * 1000);
   }
#endif
   getchar();
   return 0;
}

