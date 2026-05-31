cmake_minimum_required(VERSION 3.11)
project(arbitrage-monitor)

set(CMAKE_CXX_STANDARD 17)

find_package(ftxui REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Boost REQUIRED COMPONENTS system)

add_executable(arbitrage main.cpp)

target_link_libraries(arbitrage
    ftxui::screen
    ftxui::dom
    ftxui::component
    Boost::system
    OpenSSL::SSL
    OpenSSL::Crypto
    pthread
)