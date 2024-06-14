#include <qthread/qthread.h>
#include <stdio.h>

#include "qthreads_stdexec.hpp"
#include "argparsing.h"

int main(int argc, char **argv) {
  qthread_initialize();

  CHECK_VERBOSE();

  stdexec::sender auto s =
    stdexec::schedule(qthreads::qthreads_scheduler{}) | stdexec::then([] {
      printf("Hello from qthreads in then-functor! id = %i\n", qthread_id());
    }) |
    stdexec::bulk(20, [](int i) {
      printf("Hello from qthreads bulk! i = %i, id = %i\n", i, qthread_id());
    });
  stdexec::sync_wait(std::move(s));


  return 0;
}

/* vim:set expandtab: */
