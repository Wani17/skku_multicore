#include <iostream>
#include <sstream>
#include <thread>
#include <mutex>
#include <vector>

#define BILLION  1000000000L

using namespace std;

pthread_mutexattr_t attr;

//template<class V, int MAXLEVEL>

template<class V, int MAXLEVEL = 16>
class skiplist
{
public:
    //typedef skiplist_node<V,MAXLEVEL> NodeType;

    skiplist(V minKey, V maxKey):pHeader(-1),pTail(-1),
                                max_curr_level(1),max_level(MAXLEVEL),
                                m_minKey(minKey),m_maxKey(maxKey)
    {

    }

    void expectNodeNum(size_t nodeNum) {
        pthread_mutexattr_init(&attr);
        pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);

        nodePool.resize(nodeNum+2);
        pHeader = nodeNum;
        pTail = nodeNum+1;

        nodePool[pHeader].value = m_minKey;
        nodePool[pTail].value = m_maxKey;

        for ( int i=1; i<=MAXLEVEL; i++ ) {
            nodePool[pHeader].forwards[i] = pTail;
        }
    }

    virtual ~skiplist()
    {

    }

    int find(V searchvalue, int* preds, int* succs) {
        int lFound = -1;

        int pred = pHeader;
        //NodeType* pred = m_pHeader;
        for(int level = MAXLEVEL; level >= 1; --level) {
            int curr = nodePool[pred].forwards[level];
            while(searchvalue > nodePool[curr].value) {
                pred = curr; curr = nodePool[curr].forwards[level];
            }
            if(lFound == -1 && searchvalue == nodePool[curr].value) {
                lFound = curr;
            }
            preds[level] = pred;
            succs[level] = curr;
        }

        return lFound;
    }

    bool insert(V value, int count) {
        int topLevel = randomLevel(count);
        int preds[MAXLEVEL+1];
        int succs[MAXLEVEL+1];

        while(1) {
            int lFound = find(value, preds, succs);
            if(lFound != -1 && lFound < count) {
                return false;
            }

            int highestLocked = -1;
            int pred = -1;
            int succ = -1;
            bool valid = true;
            //cout << "#" << count << endl;
            for(int level = 1; valid && (level <= topLevel); ++level) {
                pred = preds[level];
                succ = succs[level];
                nodePool[pred].lock();
                highestLocked = level;
                valid = nodePool[pred].forwards[level] == succ;
            }
            //cout << "$" << count << endl;

            if(!valid) {
                for(int level = 1; level <= highestLocked; ++level) {
                    nodePool[preds[level]].unlock();
                }
                continue;
            }

            int newNode = count;
            nodePool[newNode].value = value;


            //NodeType* newNode = new NodeType(value, count);
            for(int level = 1; level <= topLevel; ++level) {
                nodePool[newNode].forwards[level] = succs[level];
            }
            for(int level = 1; level <= topLevel; ++level) {
                nodePool[preds[level]].forwards[level] = newNode;
            }
            nodePool[newNode].fullyLinked = true;
            //newNode->fullyLinked = true;

            //cout << "U" << count << endl;
            for(int level = 1; level <= highestLocked; ++level) {
                nodePool[preds[level]].unlock();
            }

            if(topLevel > 2) rr(count);
            return true;
        }
    }

    long query(V value, int count) {
        //rr()
        long ret = -1;
        pthread_mutex_lock(&instlock);
        while(insertCompl < pHeader && nodePool[insertCompl].fullyLinked) ++insertCompl;

        long t = insertCompl;
        if(count > insertCompl) {
            pthread_mutex_unlock(&instlock);
            return -2L;
        }

        pthread_mutex_unlock(&instlock);

        //cout << "**" << endl;

        int curr = pHeader;
        for(int level = MAXLEVEL; level >=1; level--) {
            while ( nodePool[nodePool[curr].forwards[level]].value < value ){
                curr = nodePool[curr].forwards[level];
            }
        }

        //cout << curr << endl;
        curr=nodePool[curr].forwards[1];
        //cout << curr << endl;

        if(curr >= count || value != nodePool[curr].value) return -1;

        curr=nodePool[curr].forwards[1];
        //cout << curr << endl;

        while(curr != pTail && curr >= count) {
            //cout << curr << endl;
            curr=nodePool[curr].forwards[1];
        }

        return nodePool[curr].value;
    }

    std::string printList()
    {
            int i=0;
        std::stringstream sstr;
        int currNode = nodePool[pHeader].forwards[1];
        //cout << currNode << endl;

        while ( currNode != pTail ) {
            //sstr << "(" << currNode->key << "," << currNode->value << ")" << endl;
            sstr << nodePool[currNode].value << " ";
            currNode = nodePool[currNode].forwards[1];
                i++;
            if(i>200) break;
        }
        return sstr.str();
    }

    const int max_level;

protected:
    double uniformRandom()
    {
        return rand() / double(RAND_MAX);
    }

    int randomLevel(int count) {
        int level = __builtin_ctz(count);

                return level == 0 ? 1 : min(level, MAXLEVEL-1);
                //return max(__builtin_ctz(count) + 1, MAXLEVEL);
        //double p = 0.5;
        //while ( uniformRandom() < p && level < MAXLEVEL ) {
        //    level++;
        //}
        //return level;
    }

    struct node {
        int value;
        int forwards[17];
        pthread_mutex_t mlock;
        bool fullyLinked = false;

        node() {
            pthread_mutex_init(&mlock, &attr);
        }

        void lock() {
            pthread_mutex_lock(&mlock);
        }

        void unlock() {
            pthread_mutex_unlock(&mlock);
        }
    };

    V m_minKey;
    V m_maxKey;
    int max_curr_level;

    vector<node> nodePool;
    int pHeader;
    int pTail;

    int insertCompl = 0;
    pthread_mutex_t instlock = PTHREAD_MUTEX_INITIALIZER;

    void rr(int count) {
        if(pthread_mutex_trylock(&instlock) == 0) {
            while(insertCompl < pHeader && nodePool[insertCompl].fullyLinked) ++insertCompl;
            pthread_mutex_unlock(&instlock);
        }
    }

    //skiplist_node<V,MAXLEVEL>* m_pHeader;
    //skiplist_node<V,MAXLEVEL>* m_pTail;
};
