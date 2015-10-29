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
#include <atomic>
#include <chrono>

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
                for (int num = 0; num < max_requests; ++num) {
                   std::chrono::high_resolution_clock::time_point begin = 
                       std::chrono::high_resolution_clock::now();
                   int32_t ret  = client.multiply(10, 20);
                   int64_t time = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - begin).count();
                   int32_t time_taken = time/1000000;
                   if (time_taken < min_time) min_time = time_taken;
                   if (time_taken > max_time) max_time = time_taken;
                   total_time += time_taken;
                   if (ret < 0) {
                     ++n_errors;
                   }
                   this_thread::sleep_for(chrono::milliseconds(within(200)));
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
