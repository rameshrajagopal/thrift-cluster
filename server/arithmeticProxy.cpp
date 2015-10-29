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

#define SERVER_IP  "192.168.0.241"
#define SERVER_PORT 9000

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::concurrency;

using namespace ::tutorial;
using namespace ::tutorial::arithmetic::gen;

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

int ProxySlaveTask(Queue<int> & q, Queue<int> & resQ)
{
    /* do the work */
    for (int t = 0; t < 1; ++t) {
        thread th([t, &q, &resQ]() {
            boost::shared_ptr<TTransport> socket(new TSocket(SERVER_IP, SERVER_PORT));
#ifdef NON_BLOCKING
            boost::shared_ptr<TTransport> transport(new TFramedTransport(socket));
#else
            boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
#endif
            boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
            ArithmeticServiceConcurrentClient client(protocol);
            try {
                transport->open();
                while (true) {
                   auto req = q.pop();
                   cout << "th " << t << " req: " << req << endl;
                   int32_t ret  = client.multiply(10, 20);
                   cout << "th " << t << " res: " << req << endl;
                   resQ.push(ret);
                }
                transport->close();
            } catch (TException & tx) {
                    std::cout << "ERROR: " << tx.what() << std::endl;
            }
        });
        th.detach();
    }
}

#ifdef MAIN
int main(void)
{
    Queue<int> q;
    ProxySlaveTask(q);
    for (int num = 0; num < 10; ++num) {
        q.push(num);
        usleep(100 * 1000);
    }
    getchar();
    return 0;
}
#endif
