#include "bench.hh"
#include "test_util.hh"
#include <iostream>

int main(int argc, char *argv[]) {
    uint64_t f = get_cpu_freq();
    std::cout << f << std::endl;
    CHECK_GT(f, uint64_t(0));
    std::cout << "PASS" << std::endl;
    return 0;
}
