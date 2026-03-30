#include "server/metrics_service.h"

#include "common/logger.h"
#include "metric/prometheus.h"
#include "server/fiber_runtime.h"

#include <array>
#include <boost/asio/read_until.hpp>
#include <boost/asio/streambuf.hpp>
#include <cstddef>
#include <sstream>
#include <string>

namespace idlekv {

namespace {

auto TrimCr(std::string& s) -> void {
    if (!s.empty() && s.back() == '\r') {
        s.pop_back();
    }
}

auto CloseSocket(asio::ip::tcp::socket& socket) -> void {
    boost::system::error_code ec;
    DISCARD_RESULT(socket.shutdown(asio::ip::tcp::socket::shutdown_both, ec));
    DISCARD_RESULT(socket.close(ec));
}

auto BuildHttpResponse(std::string_view status, std::string_view content_type,
                       std::string_view body) -> std::string {
    std::string response;
    response.reserve(256 + body.size());
    response += "HTTP/1.1 ";
    response += status;
    response += "\r\nContent-Type: ";
    response += content_type;
    response += "\r\nContent-Length: ";
    response += std::to_string(body.size());
    response += "\r\nConnection: close\r\n\r\n";
    return response;
}

} // namespace

auto MetricsService::Init([[maybe_unused]] EventLoop* el) -> void {}

auto MetricsService::Handle(asio::ip::tcp::socket socket) -> void {
    if (Stopped()) {
        CloseSocket(socket);
        return;
    }

    boost::system::error_code ec;
    asio::streambuf           request_buf;

    DISCARD_RESULT(asio::async_read_until(socket, request_buf, "\r\n\r\n",
                                          boost::fibers::asio::yield[ec]));
    if (ec && ec != asio::error::eof) {
        CloseSocket(socket);
        return;
    }

    std::istream request_stream(&request_buf);
    std::string  request_line;
    std::getline(request_stream, request_line);
    TrimCr(request_line);

    std::string method;
    std::string target;
    std::string version;
    std::istringstream request_line_stream(request_line);
    request_line_stream >> method >> target >> version;

    const auto query_pos = target.find('?');
    if (query_pos != std::string::npos) {
        target.erase(query_pos);
    }

    std::string status       = "200 OK";
    std::string content_type = "text/plain; version=0.0.4; charset=utf-8";
    std::string body;

    if (method != "GET") {
        status       = "405 Method Not Allowed";
        content_type = "text/plain; charset=utf-8";
        body         = "method not allowed\n";
    } else if (target != "/metrics") {
        status       = "404 Not Found";
        content_type = "text/plain; charset=utf-8";
        body         = "not found\n";
    } else {
        body = PrometheusMetrics::Instance().Render();
    }

    auto header = BuildHttpResponse(status, content_type, body);
    std::array<asio::const_buffer, 2> response{
        asio::buffer(header.data(), header.size()),
        asio::buffer(body.data(), body.size()),
    };

    DISCARD_RESULT(asio::async_write(socket, response, boost::fibers::asio::yield[ec]));
    if (ec) {
        LOG(debug, "metrics write failed: {}", ec.message());
    }

    CloseSocket(socket);
}

} // namespace idlekv
