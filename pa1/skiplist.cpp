/*
 * main.cpp
 *
 * Serial version
 *
 * Compile with -O2
 */

#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <vector>
#include <queue>
#include <condition_variable>
#define BUFFER_SIZE 4194304

#include "skiplist.h"

using namespace std;

pthread_t *threads;
pthread_mutex_t readFile = PTHREAD_MUTEX_INITIALIZER, print = PTHREAD_MUTEX_INITIALIZER;

skiplist<int> list(0,INT_MAX);

int count = 0;

//0: Ready. 1: Execute. 2: Exit
int threadVar = 0;
pthread_mutex_t threadMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t threadCond = PTHREAD_COND_INITIALIZER;

int joinVar = 0;
pthread_mutex_t joinMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t joinCond = PTHREAD_COND_INITIALIZER;


int execVar = 0;
pthread_mutex_t execMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t execCond = PTHREAD_COND_INITIALIZER;

pthread_barrier_t barrier;

struct Q {
    char action;
    long value;
    int count;

    Q() : action(0), value(0), count(0) {}
    Q(char action, long value, int count) : action(action), value(value), count(count) {}
};

vector<vector<Q>> pool;
vector<int> pLoc;
int pli = 0;
int plmt = -1;
int numThread;
//int ss[40];
//int s[40];

void* worker(void* arg) {
    int tid = *((int*) arg);

    queue<int> qq;

    int i = 0;
    int len = pool[tid].size();
    while(1) {
        //cout << "*" << endl;
        //pthread_mutex_lock(&threadMutex);
        //cout << "&" << endl;
        //if(tid == 0) sleep(3);

        int mlen = (plmt + (numThread - tid)) / numThread;
        for(; i < mlen; ++i) {
            Q& query = pool[tid][i];

            if(query.action == 'i') {
                list.insert(query.value, query.count);
            }
            else if(query.action == 'q') {
                qq.push(i);
            }

            while(!qq.empty() && qq.front() < i+100) {
                auto& t = pool[tid][qq.front()];
                long ret = list.query(t.value, t.count);

                if(ret == -2) break;

                pthread_mutex_lock(&print);
                if(ret == -1) {
                    cout << "ERROR: Not Found: " << t.value << endl;
                }
                else {
                    cout << t.value << ' ' << ret << endl;
                }
                pthread_mutex_unlock(&print);

                qq.pop();
            }

            //cout << query.first << ' ' << query.second << endl;
        }


        while(!qq.empty()) {
            auto& t = pool[tid][qq.front()];
            long ret = list.query(t.value, t.count);

            if(ret == -2) continue;

            pthread_mutex_lock(&print);
            if(ret == -1) {
                cout << "ERROR: Not Found: " << t.value << endl;
            }
            else {
                cout << t.value << ' ' << ret << endl;
            }
            pthread_mutex_unlock(&print);

            qq.pop();
        }

        pthread_barrier_wait(&barrier);

        if(tid == 0) {
            if(pli == pLoc.size()-1) {
                threadVar = 2;
                plmt = count;
            }
            else {
                plmt = pLoc[++pli];
                cout << list.printList() << endl;
            }
        }

        pthread_barrier_wait(&barrier);

        if(threadVar == 2) {
            return NULL;
        }
    }

    return NULL;
}

int main(int argc, char* argv[])
{
    struct timespec start, stop;

    // check and parse command line options
    if (argc != 3) {
        printf("Usage: %s <infile>\n", argv[0]);
        exit(EXIT_FAILURE);
    }
    char *fn = argv[1];

    numThread = atoi(argv[2]);
    threads = new pthread_t[numThread];
    pthread_barrier_init(&barrier, NULL, numThread);
    pool.resize(numThread);
    for(int i = 0; i < numThread; ++i) {
        pool[i].reserve(262144);
    }

    clock_gettime( CLOCK_REALTIME, &start);

    FILE* file = fopen(argv[1], "rb");
    // load input file
    if(!file) {
        cerr << "File does not exist: " << argv[1] << endl;
        return 1;
    }

    char* buffer = new char[BUFFER_SIZE];
    size_t bytes_read;
    char action;
    long value;
    int insertNum = 0;
    string partial;
    size_t t = 0;

    while((bytes_read = fread(buffer, 1, BUFFER_SIZE, file)) > 0) {
        size_t start = 0;
        for(size_t i = 0; i < bytes_read; ++i) {
            if(buffer[i] == '\n') {
                size_t length = i - start;

                string line;
                if(!partial.empty()) {
                    line = partial + string(buffer+start, length);
                    partial.clear();
                }
                else {
                    line = string(buffer+start, length);
                }


                istringstream iss(line);
                if (!(iss >> action >> value)) {

                    if(action == 'p') {
                        pLoc.push_back(count);
                        ++count;
                    }
                    else {
                        //printf("ERROR: Unrecognized action: '%c'\n", action);
                    }
                    start = i+1;
                    continue;
                }

                //cout << count << endl;
                Q q_item(action, value, insertNum);
                pool[t].push_back(move(q_item));

                start = i+1;
                insertNum += (action == 'i');
                ++count;
                if(++t >= numThread) {
                    t = 0;
                }
            }
        }

        if (start < bytes_read) {
            partial += string(buffer + start, bytes_read - start);
        }
    }

    fclose(file);
    if (!partial.empty()) {
        istringstream iss(partial);
        if (iss >> action >> value) {
            Q q_item(action, value, insertNum);
            insertNum += (action == 'i');
            ++count;
            pool[t].push_back(move(q_item));
            if(++t >= numThread) {
                t = 0;
            }
            //squeue.enqueue(move(q_item));
        } else {
            if(action == 'p') {
                pLoc.push_back(count);
            }
            else {
                //printf("ERROR: Unrecognized action: '%c'\n", action);
            }
            //cerr << "&&" << partial << endl;
            //std::cerr << "잘못된 형식의 라인: " << partial_line << std::endl;
        }
    }

    pLoc.push_back(count);
    //if(pLoc.empty()) plmt = count;
    plmt = pLoc[0];

    //cout << insertNum << endl;
    list.expectNodeNum(insertNum);
    for (int i = 0; i < numThread; i++) {
        int* arg = new int(i);
        int rc = pthread_create(&threads[i], NULL, worker, (void*) arg);
        if (rc) {
            cout << "Error: unable to create thread," << rc << endl;
            exit(-1);
        }
    }


    for (int i = 0; i < numThread; i++) {
        int rc = pthread_join(threads[i], NULL);
        if (rc) {
            cout << "Error: unable to join thread," << rc << endl;
            delete[] buffer;
            delete[] threads;
            exit(-1);
        }
    }

    //cout << s[0] << endl;

    //cout << ss[0] << endl;
    //cout << s[0] << ' ' << ss[0] << endl;
    //cout << "Query Hit: " << double(s[0]) / ss[0] << endl;

    delete[] buffer;
    delete[] threads;
    //cout << list.printList() << endl;

    clock_gettime( CLOCK_REALTIME, &stop);

    // print results
    double elapsed_time = (stop.tv_sec - start.tv_sec) + ((double) (stop.tv_nsec - start.tv_nsec))/BILLION ;

    cerr << "Elapsed time: " << elapsed_time << " sec" << endl;
    cerr << "Throughput: " << (double) count / elapsed_time << " ops (operations/sec)" << endl;

    return (EXIT_SUCCESS);
}

