/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/concurrency/Thread.h>
#include <thrift/concurrency/Util.h>
#include <thrift/concurrency/PlatformThreadFactory.h>

#include <iostream>
#include <vector>
#include <chrono>
#include "../gen-cpp/ArithmeticService.h"
#include <thread>
#include "queue.h"
#include "response.h"
#include "request.h"
#include <atomic>

#define SERVER_IP  "192.168.0.241"
#define SERVER_PORT 9000
#define within(num) (int) ((float) num * random() / (RAND_MAX + 1.0))

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::concurrency;

using namespace ::tutorial;
using namespace ::tutorial::arithmetic::gen;
static std::atomic<int> total_time {0};
static std::atomic<int> n_errors {0};
static std::atomic<int> min_time {INT_MAX};
static std::atomic<int> max_time {INT_MIN};

class ProxySlave
{
public:
    ProxySlave(int num, const string addr, const int port):
        id(num),
        socket(new TSocket(addr, port)), 
        transport(new TFramedTransport(socket)), 
        protocol(new TBinaryProtocol(transport)),
        client(protocol)
    {
        cout << "Slave at " << addr << ":" << port << endl;
    }
    void task() 
    {
        cout << "Slave Task  " << id << endl;
        try {
            transport->open();
            while (true) { 
                auto req = q.pop();
                cout << "got the request" << endl;
                int32_t ret  = client.multiply(10, 20);
                if (ret > 0) ret = 0;
                req->updateResponse(id, ret);
                cout << "response: " << ret << endl;
            }
            transport->close();
        } catch(TException & tx) {
            std::cout << "ERROR: " << tx.what() << std::endl;
        }
    }
    void send(shared_ptr<Request> req)
    {
        cout << "Request sending to slave" << endl;
        q.push(req);
    }
private:
    int id;
    boost::shared_ptr<TTransport> socket;
    boost::shared_ptr<TTransport> transport;
    boost::shared_ptr<TProtocol> protocol;
    ArithmeticServiceConcurrentClient client;
    Queue<shared_ptr<Request>> q;
};

void processRequest(int reqNum, const string & req, vector<shared_ptr<ProxySlave>> & slaves)
{
    Response res(slaves.size());
    shared_ptr<Request> request = make_shared<Request> (reqNum, req, res);

    for (shared_ptr<ProxySlave> & slave: slaves) {
        slave->send(request);
    }
    res.wait();
    /* analyse the error and do whatever needs to be done */
}

shared_ptr<ProxySlave> getProxySlave(const string & addr, const int port, int id)
{
    shared_ptr<ProxySlave> slave = make_shared<ProxySlave> (id, addr, port);
    thread th([slave]() {
       slave->task();
    });
    th.detach();
}

int getDiffTime(struct timeval tstart, struct timeval tend)
{
    struct timeval tdiff;

    //cout << "v " << tstart.tv_sec << " " << tstart.tv_usec << endl;
    if (tend.tv_usec < tstart.tv_usec) {
        tdiff.tv_sec = tend.tv_sec - tstart.tv_sec - 1;
        tdiff.tv_usec = 1000000 + tend.tv_usec - tstart.tv_usec;
    } else {
        tdiff.tv_sec = tend.tv_sec - tstart.tv_sec;
        tdiff.tv_usec = tend.tv_usec - tstart.tv_usec;
    }
    return (tdiff.tv_sec * 1000 + (tdiff.tv_usec/1000));
}

int ProxySlaveTask(const char * host, int port, int max_threads, int max_requests)
{
    /* do the work */
    for (int t = 0; t < max_threads; ++t) {
        thread th([t, host, port, max_requests]() {
            boost::shared_ptr<TTransport> socket(new TSocket(host, port));
#ifdef NON_BLOCKING
            boost::shared_ptr<TTransport> transport(new TFramedTransport(socket));
#else
            boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
#endif
            boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
            ArithmeticServiceConcurrentClient client(protocol);
            srandom((unsigned) time(NULL));
            try {
                transport->open();
                struct timeval starttime, endtime;
                for (int num = 0; num < max_requests; ++num) {
                   gettimeofday(&starttime, NULL);
                   int32_t ret  = client.multiply(10, 20);
                   gettimeofday(&endtime, NULL);
                   int time_taken = getDiffTime(starttime, endtime);
                   if (time_taken < min_time) min_time = time_taken;
                   if (time_taken > max_time) max_time = time_taken;
                   total_time += time_taken;
                   if (ret < 0) {
                     ++n_errors;
                   }
                   usleep(within(400 * 1000));
                }
                transport->close();
            } catch (TException & tx) {
                    std::cout << "ERROR: " << tx.what() << std::endl;
            }
        });
        th.detach();
    }
}

void printStats(int max_requests, int max_threads)
{
    cout << "requests per user: " << max_requests << " #users " << max_threads << endl;
    cout << "total time taken: " << total_time << " msec " << endl;
    cout << "num of errors : " << n_errors << endl;
    cout << "slowest response: " << max_time <<  " msec " << endl;
    cout << "fastest response: " << min_time <<  " mesc " << endl;
}

int main(int argc, char * argv[])
{
    if (argc != 5) {
        cout << "Usage: " << endl;
        cout << argv[0] << " " << "masterip port concurrency repeat" << endl;
        return -1;
    }
    ProxySlaveTask(argv[1], atoi(argv[2]), atoi(argv[3]), atoi(argv[4]));
    getchar();
    printStats(atoi(argv[3]), atoi(argv[4]));
    return 0;
}
