# ⚡ Real-Time Crypto Arbitrage Monitor

A real-time cryptocurrency arbitrage detection system built in C++ that monitors live BTC/USD prices across Binance and Kraken simultaneously, detects price discrepancies, and displays a live terminal dashboard.

## Features
- Live WebSocket connections to Binance and Kraken simultaneously
- Real-time spread calculation and arbitrage detection
- Interactive terminal dashboard built with FTXUI
- Multi-threaded architecture with mutex-protected shared state
- Alerts logged to CSV with timestamps

## Tech Stack
- Language: C++17
- Networking: Boost.Beast + OpenSSL
- UI: FTXUI Terminal Dashboard
- Concurrency: std::thread + std::mutex

## How to Build & Run
```bash
sudo apt install libboost-all-dev libssl-dev cmake -y
mkdir build && cd build
cmake ..
make -j4
./arbitrage
```

## Architecture
Thread 1 → Binance WebSocket ──┐
├──► Mutex-Protected Price Map ──► Arbitrage Checker
Thread 2 → Kraken WebSocket ───┘
