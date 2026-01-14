#include <iostream>
#include <memory>
#include <signal.h>

#include "spdlog/spdlog.h"

std::string banner() {
    return R"(
    _     _ _      _  ___     __
   (_) __| | | ___| |/ \ \   / /
   | |/ _` | |/ _ \ ' /\ \ / / 
   | | (_| | |  __/ . \ \ V /  
   |_|\__,_|_|\___|_|\_\ \_/   
)";
}

int main(int argc, char* argv[]) {
    banner();

    return 0;
}