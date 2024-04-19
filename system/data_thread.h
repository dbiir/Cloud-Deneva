#ifndef _DATATHREAD_H_
#define _DATATHREAD_H_

#include "global.h"
#include "thread.h"

class Workload;

class DataThread : public Thread {
public:
    RC run();
    void setup();
};

#endif