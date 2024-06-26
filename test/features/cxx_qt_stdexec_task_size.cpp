#include <chrono>
#include <format>
#include <iostream>
#include <qthread/qthread.h>
#include <stdio.h>

#include "argparsing.h"
#include "qthreads_stdexec.hpp"

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

void task(double task_size_s) noexcept {
  auto t = std::chrono::steady_clock::now();
  while (std::chrono::duration<double>(std::chrono::steady_clock::now() - t) <
         std::chrono::duration<double>(task_size_s)) {
    __asm__ __volatile__("rep; nop" : : : "memory");
  }
}

// The "task" method simply spawns total_tasks independent tasks without any
// special consideration for grouping or affinity.
void do_work_task(std::size_t num_threads, std::uint64_t tasks_per_thread,
                  double task_size_s) {
  auto const total_tasks = num_threads * tasks_per_thread;
  auto sched = qthreads::qthreads_scheduler{};
  auto spawn = [=]() {
    return stdexec::schedule(sched) |
           stdexec::then([&] { task(task_size_s); }) |
           stdexec::ensure_started() | stdexec::then([](auto &&...) {});
  };

  std::vector<decltype(spawn())> senders;
  senders.reserve(total_tasks);

  for (std::uint64_t i = 0; i < total_tasks; ++i) {
    senders.push_back(spawn());
  }
  for (std::uint64_t i = 0; i < total_tasks; ++i) {
    stdexec::sync_wait(std::move(senders[i]));
  }
}

// The "barrier" method spawns one task per worker thread and uses a barrier run
// the "tasks" in lockstep on each worker thread. void
// do_work_barrier(std::uint64_t tasks_per_thread, double task_size_s)
// {
//     auto const num_threads = pika::get_num_worker_threads();
//     auto sched = qthreads_scheduler{};
//     // TODO
//     // pika::barrier b(num_threads);
//     auto work = [=, &b]() {
//         for (std::uint64_t i = 0; i < tasks_per_thread; ++i)
//         {
//             b.arrive_and_wait();
//             task(task_size_s);
//         }
//     };
//     auto spawn = [=]() {
//         return ex::schedule(sched) | ex::then(work) | ex::ensure_started() |
//         ex::drop_value();
//     };

//     std::vector<decltype(spawn())> senders;
//     senders.reserve(num_threads);

//     for (std::uint64_t i = 0; i < num_threads; ++i) {
//     senders.push_back(spawn()); }
//     tt::sync_wait(ex::when_all_vector(std::move(senders)));
// }

// The "bulk" method sequences each set of num_worker_threads tasks through
// bulk. void do_work_bulk(std::uint64_t tasks_per_thread, double task_size_s)
// {
//     auto const num_threads = pika::get_num_worker_threads();
//     auto sched = qthreads_scheduler{};
//     pika::barrier b(num_threads);
//     auto work = [=](auto) { task(task_size_s); };

//     ex::unique_any_sender<> sender{ex::just()};
//     for (std::uint64_t i = 0; i < tasks_per_thread; ++i)
//     {
//         sender = std::move(sender) | ex::transfer(sched) |
//         ex::bulk(num_threads, work);
//     }
//     tt::sync_wait(std::move(sender));
// }

int main(int argc, char *argv[]) {
  if (argc != 7) {
    std::cerr
        << "usage: exec <tasks-per-thread> <task-size-min-s> <task-size-max-s> "
           "<task-size-growth-factor> <target-efficiency> "
           "<num-threads-requested>\n";
    std::terminate();
  }

  std::string const method{"task"};
  std::uint64_t const tasks_per_thread = std::stoul(argv[1]);
  double const task_size_min_s = std::stod(argv[2]);
  double const task_size_max_s = std::stod(argv[3]);
  double const task_size_growth_factor = std::stod(argv[4]);
  double const target_efficiency = std::stod(argv[5]);
  // NOTE: this is just a side channel for informing the application about the
  // number of threads. The actual number of threads is set by Qthreads
  // environment variables.
  std::size_t const num_threads = std::stoul(argv[6]);

  qthread_initialize();

  using do_work_type = void(std::size_t, std::uint64_t, double);
  do_work_type *do_work = [&]() {
    if (method == "task") {
      return do_work_task;
    }
    // else if (method == "barrier") { return do_work_barrier; }
    // else if (method == "bulk") { return do_work_bulk; }
    else {
      std::terminate();
      // PIKA_THROW_EXCEPTION(pika::error::bad_parameter, "task_size",
      //     "--method must be \"task\", \"barrier\", or \"bulk\" ({} given)",
      //     method);
    }
  }();

  if (task_size_min_s <= 0) {
    std::terminate();
    // PIKA_THROW_EXCEPTION(pika::error::bad_parameter, "task_size",
    //     "--task-size-min-s must be strictly larger than zero ({} given)",
    //     task_size_min_s);
  }

  if (task_size_max_s <= 0) {
    std::terminate();
    // PIKA_THROW_EXCEPTION(pika::error::bad_parameter, "task_size",
    //     "--task-size-max-s must be strictly larger than zero ({} given)",
    //     task_size_max_s);
  }

  if (task_size_max_s <= task_size_min_s) {
    std::terminate();
    // PIKA_THROW_EXCEPTION(pika::error::bad_parameter, "task_size",
    //     "--task-size-max-s must be strictly larger than --task-size-min-s ({}
    //     and {} given, " "respectively)", task_size_max_s, task_size_min_s);
  }

  if (task_size_growth_factor <= 1) {
    std::terminate();
    // PIKA_THROW_EXCEPTION(pika::error::bad_parameter, "task_size",
    //     "--task-size-growth-factor must be strictly larger than one ({}
    //     given)", task_size_growth_factor);
  }

  if (target_efficiency <= 0 || target_efficiency >= 1) {
    std::terminate();
    // PIKA_THROW_EXCEPTION(pika::error::bad_parameter, "task_size",
    //     "--target-efficiency must be strictly between 0 and 1 ({} given)",
    //     target_efficiency);
  }

  auto const total_tasks = num_threads * tasks_per_thread;

  double task_size_s = task_size_min_s;
  double efficiency = 0.0;

  std::cout
      << "method,num_threads,tasks_per_thread,task_size_s,single_threaded_"
         "reference_time_s,time_s,task_overhead_time_s,efficiency\n";

  do {
    double const single_threaded_reference_time_s = total_tasks * task_size_s;

    auto t = std::chrono::steady_clock::now();
    do_work(num_threads, tasks_per_thread, task_size_s);
    double time_s =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t)
            .count();

    efficiency = single_threaded_reference_time_s / time_s / num_threads;
    double task_overhead_time_s =
        (time_s - single_threaded_reference_time_s / num_threads) /
        tasks_per_thread;
    std::cout << std::format("{},{},{},{:.9f},{:.9f},{:.9f},{:.9f},{:.4f}\n",
                             method, num_threads, tasks_per_thread, task_size_s,
                             single_threaded_reference_time_s, time_s,
                             task_overhead_time_s, efficiency);

    task_size_s *= task_size_growth_factor;
  } while (efficiency < target_efficiency && task_size_s < task_size_max_s);
}
