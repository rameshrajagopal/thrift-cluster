// This autogenerated skeleton file illustrates how to build a server.
// You should copy it to another filename to avoid overwriting it.

#include "ArithmeticService.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/server/TNonblockingServer.h>

#include <unistd.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

#define MAX_NUM_THREADS (10)
#define DBG_PRINT(fmt...) printf(fmt)

using boost::shared_ptr;

using namespace  ::tutorial::arithmetic::gen;

class ArithmeticServiceHandler : virtual public ArithmeticServiceIf {
 public:
  ArithmeticServiceHandler() {
    // Your initialization goes here
  }

  int64_t add(const int32_t num1, const int32_t num2) {
      DBG_PRINT("%s: %d %d\n", __FUNCTION__, num1, num2);
      usleep(500 * 1000); 
      return num1 + num2;
  }

  int64_t multiply(const int32_t num1, const int32_t num2) {
      DBG_PRINT("%s: %d %d\n", __FUNCTION__, num1, num2);
      usleep(1000 * 1000);
      return num1 * num2;
  }

};

int main(int argc, char **argv) {
  int port = 9000;
  shared_ptr<ArithmeticServiceHandler> handler(new ArithmeticServiceHandler());
  shared_ptr<TProcessor> processor(new ArithmeticServiceProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

  shared_ptr<ThreadManager> threadManager =  ThreadManager::newSimpleThreadManager(MAX_NUM_THREADS);
  shared_ptr<ThreadFactory> threadFactory(new PosixThreadFactory());
  threadManager->threadFactory(threadFactory);
  threadManager->start();

  //TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
  TNonblockingServer server(processor, protocolFactory, port, threadManager);
  server.setNumIOThreads(2);
  server.serve();
  return 0;
}

