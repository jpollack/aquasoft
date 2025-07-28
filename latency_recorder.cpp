#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <ctime>
#include <thread>
#include <chrono>
#include <hdr/hdr_histogram.h>
#include <hdr/hdr_histogram_log.h>

int main() {
    // 1) Initialize a histogram tracking [1μs–1 000 000μs] with 3 significant digits
    struct hdr_histogram* hist = nullptr;
    if (hdr_init(1, 1000000, 3, &hist) != 0) {
        std::fprintf(stderr, "Failed to initialize HDR histogram\n");
        return 1;
    }
    // 2) Prepare two output files: one for binary HDR-log, one for human-readable percentiles
    FILE* log_out  = std::fopen("latency_log.hdr", "w");
    if (!log_out) {
        std::perror("fopen");
        return 1;
    }
    // 3) Set up the log writer and write a header with a tag
    struct hdr_log_writer writer;
    hdr_log_writer_init(&writer);
    hdr_timespec ts_start;
    clock_gettime(CLOCK_REALTIME, &ts_start);
    hdr_log_write_header(&writer, log_out, "latency_measurements", &ts_start);
    // (recording & logging example flow based on hiccup example) :contentReference[oaicite:1]{index=1}

    // 4) Simulate 100 measurements
    for (int i = 0; i < 10000; ++i) {
      hdr_timespec t0, t1;
        clock_gettime(CLOCK_REALTIME, &t0);

        // synthetic work: sleep 100μs
        std::this_thread::sleep_for(
            std::chrono::microseconds(120)
        );

        clock_gettime(CLOCK_REALTIME, &t1);
        // compute latency in μs
        int64_t us = (t1.tv_sec  - t0.tv_sec ) * 1000000
                   + (t1.tv_nsec - t0.tv_nsec) / 1000;
        // record with coordinated-omission correction
        hdr_record_corrected_value(hist, us, 1000);

        // write one interval record to the HDR-log
        hdr_log_write(&writer, log_out, &t0, &t1, hist);
    }

    FILE* text_out = std::fopen("latency_percentiles.txt", "w");
    if (!text_out) {
        std::perror("fopen");
        return 1;
    }
    // 5) Dump percentiles (5 ticks per half-distance) in "CLASSIC" text format
    hdr_percentiles_print(hist, text_out, 5, 1.0, CLASSIC);
    // (usage from Simple Tutorial) :contentReference[oaicite:2]{index=2}

    // cleanup
    std::fclose(log_out);
    std::fclose(text_out);
    hdr_close(hist);
    return 0;
}
