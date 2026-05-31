#include <iostream>
#include <string>
#include <map>
#include <mutex>
#include <thread>
#include <sstream>
#include <iomanip>
#include <fstream>
#include <chrono>
#include <atomic>
#include <functional>

#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>

#include "ftxui/component/component.hpp"
#include "ftxui/component/screen_interactive.hpp"
#include "ftxui/dom/elements.hpp"

namespace beast     = boost::beast;
namespace websocket = beast::websocket;
namespace net       = boost::asio;
namespace ssl       = net::ssl;
using tcp           = net::ip::tcp;
using namespace ftxui;

// ============================================================
//  SHARED STATE
// ============================================================

std::mutex priceMutex;
std::map<std::string, double> prices;   // "binance" -> price, "kraken" -> price
std::vector<std::string> alertLog;
std::atomic<bool> running{true};
double spreadThreshold = 5.0;           // alert if spread > $5

// ============================================================
//  CSV LOGGER
// ============================================================

void logAlert(double binance, double kraken, double spread, double spreadPct) {
    std::ofstream file("alerts.csv", std::ios::app);
    auto now = std::chrono::system_clock::now();
    auto t   = std::chrono::system_clock::to_time_t(now);
    file << std::put_time(std::localtime(&t), "%Y-%m-%d %H:%M:%S")
         << "," << binance
         << "," << kraken
         << "," << std::fixed << std::setprecision(2) << spread
         << "," << std::setprecision(4) << spreadPct
         << "\n";
}

// ============================================================
//  SIMPLE JSON VALUE EXTRACTOR
// ============================================================

std::string extractJson(const std::string& json, const std::string& key) {
    std::string search = "\"" + key + "\":\"";
    auto pos = json.find(search);
    if (pos == std::string::npos) {
        search = "\"" + key + "\":";
        pos = json.find(search);
        if (pos == std::string::npos) return "";
        pos += search.size();
        auto end = json.find_first_of(",}", pos);
        return json.substr(pos, end - pos);
    }
    pos += search.size();
    auto end = json.find("\"", pos);
    return json.substr(pos, end - pos);
}

// ============================================================
//  BINANCE WEBSOCKET
// ============================================================

void connectBinance() {
    try {
        net::io_context ioc;
        ssl::context ctx{ssl::context::tlsv12_client};
        ctx.set_default_verify_paths();

        tcp::resolver resolver{ioc};
        websocket::stream<ssl::stream<tcp::socket>> ws{ioc, ctx};

        auto results = resolver.resolve("stream.binance.com", "9443");
        net::connect(beast::get_lowest_layer(ws), results);

        if (!SSL_set_tlsext_host_name(ws.next_layer().native_handle(),
                                       "stream.binance.com"))
            return;

        ws.next_layer().handshake(ssl::stream_base::client);
        ws.handshake("stream.binance.com", "/ws/btcusdt@ticker");

        while (running) {
            beast::flat_buffer buffer;
            ws.read(buffer);
            std::string msg = beast::buffers_to_string(buffer.data());

            std::string val = extractJson(msg, "c");
            if (!val.empty()) {
                try {
                    double price = std::stod(val);
                    std::lock_guard<std::mutex> lock(priceMutex);
                    prices["binance"] = price;
                } catch (...) {}
            }
        }
    } catch (std::exception& e) {
        std::lock_guard<std::mutex> lock(priceMutex);
        alertLog.push_back("Binance error: " + std::string(e.what()));
    }
}

// ============================================================
//  KRAKEN WEBSOCKET
// ============================================================

void connectKraken() {
    try {
        net::io_context ioc;
        ssl::context ctx{ssl::context::tlsv12_client};
        ctx.set_default_verify_paths();
        ctx.set_verify_mode(ssl::verify_none);

        tcp::resolver resolver{ioc};
        websocket::stream<ssl::stream<tcp::socket>> ws{ioc, ctx};

        auto results = resolver.resolve("ws.kraken.com", "443");
        net::connect(beast::get_lowest_layer(ws), results);

        if (!SSL_set_tlsext_host_name(ws.next_layer().native_handle(),
                                       "ws.kraken.com"))
            return;

        ws.next_layer().handshake(ssl::stream_base::client);

        ws.set_option(websocket::stream_base::decorator(
            [](websocket::request_type& req) {
                req.set(beast::http::field::user_agent, "ArbitrageMonitor/1.0");
            }));

        ws.handshake("ws.kraken.com", "/");

        // Kraken v2 subscription
        std::string sub = R"({"method":"subscribe","params":{"channel":"ticker","symbol":["BTC/USD"]}})";
        ws.write(net::buffer(sub));

        while (running) {
            beast::flat_buffer buffer;
            ws.read(buffer);
            std::string msg = beast::buffers_to_string(buffer.data());

            // Skip heartbeat and status messages
            if (msg.find("\"channel\":\"ticker\"") == std::string::npos) continue;
            if (msg.find("\"type\":\"snapshot\"") == std::string::npos &&
                msg.find("\"type\":\"update\"")   == std::string::npos) continue;

            // Extract last trade price from "last":XXXXX.XX
            auto pos = msg.find("\"last\":");
            if (pos != std::string::npos) {
                pos += 7;
                auto end = msg.find_first_of(",}", pos);
                if (end != std::string::npos) {
                    try {
                        double price = std::stod(msg.substr(pos, end - pos));
                        std::lock_guard<std::mutex> lock(priceMutex);
                        prices["kraken"] = price;
                    } catch (...) {}
                }
            }
        }
    } catch (std::exception& e) {
        std::lock_guard<std::mutex> lock(priceMutex);
        alertLog.push_back("Kraken error: " + std::string(e.what()));
    }
}

// ============================================================
//  ARBITRAGE CHECKER
// ============================================================

void checkArbitrage() {
    while (running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        std::lock_guard<std::mutex> lock(priceMutex);

        if (prices.count("binance") && prices.count("kraken")) {
            double b = prices["binance"];
            double k = prices["kraken"];
            double spread    = std::abs(b - k);
            double spreadPct = (spread / std::min(b, k)) * 100.0;

            if (spread > spreadThreshold) {
                auto now = std::chrono::system_clock::now();
                auto t   = std::chrono::system_clock::to_time_t(now);
                std::ostringstream ss;
                ss << std::put_time(std::localtime(&t), "%H:%M:%S")
                   << "  SPREAD: $" << std::fixed << std::setprecision(2)
                   << spread << " (" << std::setprecision(4)
                   << spreadPct << "%)";
                alertLog.push_back(ss.str());
                if (alertLog.size() > 20)
                    alertLog.erase(alertLog.begin());
                logAlert(b, k, spread, spreadPct);
            }
        }
    }
}

// ============================================================
//  DASHBOARD
// ============================================================

int main() {
    // Write CSV header
    std::ofstream file("alerts.csv");
    file << "timestamp,binance,kraken,spread_usd,spread_pct\n";
    file.close();

    // Start threads
    std::thread t1(connectBinance);
    std::thread t2(connectKraken);
    std::thread t3(checkArbitrage);

    auto screen = ScreenInteractive::TerminalOutput();

    auto component = Renderer([&] {
        // Read shared state
        double binPrice = 0, krPrice = 0, spread = 0, spreadPct = 0;
        std::vector<std::string> logs;
        {
            std::lock_guard<std::mutex> lock(priceMutex);
            if (prices.count("binance")) binPrice = prices["binance"];
            if (prices.count("kraken"))  krPrice  = prices["kraken"];
            spread    = std::abs(binPrice - krPrice);
            spreadPct = (binPrice && krPrice)
                        ? (spread / std::min(binPrice, krPrice)) * 100.0
                        : 0;
            logs = alertLog;
        }

        auto fmt = [](double v) {
            std::ostringstream ss;
            ss << std::fixed << std::setprecision(2) << v;
            return ss.str();
        };

        // Status color
        auto spreadColor = spread > spreadThreshold
                           ? color(Color::GreenLight)
                           : color(Color::GrayDark);

        // Alert log elements
        Elements logElements;
        for (auto it = logs.rbegin(); it != logs.rend(); ++it)
            logElements.push_back(
                text(*it) | color(Color::Yellow) | size(HEIGHT, EQUAL, 1));
        if (logElements.empty())
            logElements.push_back(
                text("  Monitoring... alerts will appear here")
                | color(Color::GrayDark));

        return vbox({
            // Title
            text(" Real-Time Crypto Arbitrage Monitor ")
                | bold | color(Color::Cyan) | center,
            text(" BTC/USD  |  Binance vs Kraken  |  Live ")
                | color(Color::GrayDark) | center,
            separator(),

            // Price boxes
            hbox({
                vbox({
                    text(" BINANCE ") | bold | color(Color::Yellow) | center,
                    separator(),
                    text(" $" + fmt(binPrice)) | bold
                        | color(binPrice ? Color::White : Color::GrayDark)
                        | center,
                    text(binPrice ? " LIVE " : " connecting... ")
                        | color(binPrice ? Color::Green : Color::GrayDark)
                        | center,
                }) | border | flex,

                vbox({
                    text(" KRAKEN ") | bold | color(Color::Magenta) | center,
                    separator(),
                    text(" $" + fmt(krPrice)) | bold
                        | color(krPrice ? Color::White : Color::GrayDark)
                        | center,
                    text(krPrice ? " LIVE " : " connecting... ")
                        | color(krPrice ? Color::Green : Color::GrayDark)
                        | center,
                }) | border | flex,

                vbox({
                    text(" SPREAD ") | bold | color(Color::Cyan) | center,
                    separator(),
                    text(" $" + fmt(spread)) | bold | spreadColor | center,
                    text(" " + fmt(spreadPct) + "% ")
                        | spreadColor | center,
                }) | border | flex,
            }),

            separator(),

            // Alert log
            vbox({
                text(" ARBITRAGE ALERTS (threshold: $"
                     + fmt(spreadThreshold) + ") ")
                    | bold | color(Color::Cyan),
                separator(),
                vbox(logElements),
            }) | border,

            separator(),
            text(" q = quit  |  alerts saved to alerts.csv ")
                | color(Color::GrayDark) | center,
        });
    });

    // Refresh every 500ms
    std::thread refresher([&] {
        while (running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            screen.PostEvent(Event::Custom);
        }
    });

    auto final = CatchEvent(component, [&](Event e) {
        if (e == Event::Character('q')) {
            running = false;
            screen.ExitLoopClosure()();
            return true;
        }
        return false;
    });

    screen.Loop(final);

    running = false;
    t1.detach(); t2.detach(); t3.detach();
    refresher.detach();
    return 0;
}