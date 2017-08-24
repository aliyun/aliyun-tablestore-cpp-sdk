/* 
BSD 3-Clause License

Copyright (c) 2017, Alibaba Cloud
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
#include "src/tablestore/core/http/response_reader.hpp"
#include "tablestore/core/types.hpp"
#include "tablestore/util/logger.hpp"
#include "tablestore/util/foreach.hpp"
#include "tablestore/util/move.hpp"
#include "tablestore/util/prettyprint.hpp"
#include "testa/testa.hpp"
#include <tr1/tuple>
#include <deque>
#include <string>
#include <memory>

using namespace std;
using namespace std::tr1;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

void ResponseReader_NoBody(const string&)
{
    auto_ptr<Logger> logger(createLogger("/", Logger::kDebug));
    Tracker tracker("tracker");
    http::ResponseReader rr(*logger, tracker);
    string response = "HTTP/1.1 200 OK\r\n"
        "Content-Length: 0\r\n"
        "\r\n";
    http::ResponseReader::RequireMore more = http::ResponseReader::REQUIRE_MORE;
    Optional<OTSError> err = rr.feed(more, MemPiece::from(response));
    TESTA_ASSERT(!err.present())
        (*err).issue();
    TESTA_ASSERT(more == http::ResponseReader::STOP).issue();
    TESTA_ASSERT(rr.httpStatus() == 200)
        (rr.httpStatus()).issue();
    http::InplaceHeaders headers;
    moveAssign(headers, util::move(rr.mutableHeaders()));
    deque<tuple<MemPiece, MemPiece> > hs;
    FOREACH_ITER(i, headers) {
        hs.push_back(make_tuple(i->first, i->second));
    }
    TESTA_ASSERT(pp::prettyPrint(hs) == "[(b\"Content-Length\",b\"0\")]")
        (hs).issue();
    deque<MemPiece> body;
    moveAssign(body, util::move(rr.mutableBody()));
    TESTA_ASSERT(pp::prettyPrint(body) == "[]")
        (body).issue();
}
TESTA_DEF_JUNIT_LIKE1(ResponseReader_NoBody);

void ResponseReader_TooLongStatusLine(const string&)
{
    auto_ptr<Logger> logger(createLogger("/", Logger::kDebug));
    Tracker tracker("tracker");
    http::ResponseReader rr(*logger, tracker);
    string response0 = "HTTP/1.1 ";
    string response1 = "200 OK\r\n"
        "Content-Length: 0\r\n"
        "\r\n";
    http::ResponseReader::RequireMore more = http::ResponseReader::REQUIRE_MORE;
    Optional<OTSError> err = rr.feed(more, MemPiece::from(response0));
    TESTA_ASSERT(!err.present())
        (*err).issue();
    err = rr.feed(more, MemPiece::from(response1));
    TESTA_ASSERT(err.present()).issue();
    TESTA_ASSERT(pp::prettyPrint(*err) ==
        "{\"HttpStatus\": 56, "
        "\"ErrorCode\": \"OTSCorruptedResponse\", "
        "\"Message\": \"HTTP response is corrupted: too long status line.\"}")
        (*err).issue();
}
TESTA_DEF_JUNIT_LIKE1(ResponseReader_TooLongStatusLine);

void ResponseReader_HttpVer(const string&)
{
    auto_ptr<Logger> logger(createLogger("/", Logger::kDebug));
    Tracker tracker("tracker");
    http::ResponseReader rr(*logger, tracker);
    string response = "FTP 200 OK\r\n"
        "Content-Length: 0\r\n"
        "\r\n";
    http::ResponseReader::RequireMore more = http::ResponseReader::REQUIRE_MORE;
    Optional<OTSError> err = rr.feed(more, MemPiece::from(response));
    TESTA_ASSERT(err.present()).issue();
    TESTA_ASSERT(pp::prettyPrint(*err) ==
        "{\"HttpStatus\": 56, "
        "\"ErrorCode\": \"OTSCorruptedResponse\", "
        "\"Message\": \"HTTP response is corrupted: status line syntax is incorrect.\"}")
        (*err).issue();
}
TESTA_DEF_JUNIT_LIKE1(ResponseReader_HttpVer);

void ResponseReader_HttpStatus(const string&)
{
    auto_ptr<Logger> logger(createLogger("/", Logger::kDebug));
    Tracker tracker("tracker");
    http::ResponseReader rr(*logger, tracker);
    string response = "HTTP/1.1 OK\r\n"
        "Content-Length: 0\r\n"
        "\r\n";
    http::ResponseReader::RequireMore more = http::ResponseReader::REQUIRE_MORE;
    Optional<OTSError> err = rr.feed(more, MemPiece::from(response));
    TESTA_ASSERT(err.present()).issue();
    TESTA_ASSERT(pp::prettyPrint(*err) ==
        "{\"HttpStatus\": 56, "
        "\"ErrorCode\": \"OTSCorruptedResponse\", "
        "\"Message\": \"HTTP response is corrupted: status line syntax is incorrect.\"}")
        (*err).issue();
}
TESTA_DEF_JUNIT_LIKE1(ResponseReader_HttpStatus);

void ResponseReader_ContentLength(const string&)
{
    auto_ptr<Logger> logger(createLogger("/", Logger::kDebug));
    Tracker tracker("tracker");
    http::ResponseReader rr(*logger, tracker);
    string response0 = "HTTP/1.1 200 OK\r\n"
        "Content-Length: 2\r\n"
        "\r\n"
        "a";
    string response1 = "b";
    http::ResponseReader::RequireMore more = http::ResponseReader::REQUIRE_MORE;
    Optional<OTSError> err = rr.feed(more, MemPiece::from(response0));
    TESTA_ASSERT(!err.present())
        (*err).issue();
    TESTA_ASSERT(more == http::ResponseReader::REQUIRE_MORE).issue();

    err = rr.feed(more, MemPiece::from(response1));
    TESTA_ASSERT(!err.present())
        (*err).issue();
    TESTA_ASSERT(more == http::ResponseReader::STOP).issue();
    deque<MemPiece> body;
    moveAssign(body, util::move(rr.mutableBody()));
    TESTA_ASSERT(pp::prettyPrint(body) == "[b\"a\",b\"b\"]")
        (body).issue();
}
TESTA_DEF_JUNIT_LIKE1(ResponseReader_ContentLength);

void ResponseReader_Chunked(const string&)
{
    auto_ptr<Logger> logger(createLogger("/", Logger::kDebug));
    Tracker tracker("tracker");
    http::ResponseReader rr(*logger, tracker);
    string response = "HTTP/1.1 200 OK\r\n"
        "Transfer-Encoding: chunked\r\n"
        "\r\n"
        "5\r\n"
        "abcde\r\n"
        "0\r\n"
        "\r\n";
    http::ResponseReader::RequireMore more = http::ResponseReader::REQUIRE_MORE;
    Optional<OTSError> err = rr.feed(more, MemPiece::from(response));
    TESTA_ASSERT(!err.present())
        (*err).issue();
    TESTA_ASSERT(more == http::ResponseReader::STOP).issue();
    deque<MemPiece> body;
    moveAssign(body, util::move(rr.mutableBody()));
    TESTA_ASSERT(pp::prettyPrint(body) == "[b\"abcde\"]")
        (body).issue();
}
TESTA_DEF_JUNIT_LIKE1(ResponseReader_Chunked);

void ResponseReader_MultipleChunked(const string&)
{
    auto_ptr<Logger> logger(createLogger("/", Logger::kDebug));
    Tracker tracker("tracker");
    http::ResponseReader rr(*logger, tracker);
    string response = "HTTP/1.1 200 OK\r\n"
        "Transfer-Encoding: chunked\r\n"
        "\r\n"
        "2\r\n"
        "ab\r\n"
        "3\r\n"
        "cde\r\n"
        "0\r\n"
        "\r\n";
    http::ResponseReader::RequireMore more = http::ResponseReader::REQUIRE_MORE;
    Optional<OTSError> err = rr.feed(more, MemPiece::from(response));
    TESTA_ASSERT(!err.present())
        (*err).issue();
    TESTA_ASSERT(more == http::ResponseReader::STOP).issue();
    deque<MemPiece> body;
    moveAssign(body, util::move(rr.mutableBody()));
    TESTA_ASSERT(pp::prettyPrint(body) == "[b\"ab\",b\"cde\"]")
        (body).issue();
}
TESTA_DEF_JUNIT_LIKE1(ResponseReader_MultipleChunked);

void ResponseReader_LongChunk_lowercase(const string&)
{
    auto_ptr<Logger> logger(createLogger("/", Logger::kDebug));
    Tracker tracker("tracker");
    http::ResponseReader rr(*logger, tracker);
    string response = "HTTP/1.1 200 OK\r\n"
        "Transfer-Encoding: chunked\r\n"
        "\r\n"
        "b4\r\n"
        "0123456789abcdefghijklmnopqrstuvwxyz"
        "0123456789abcdefghijklmnopqrstuvwxyz"
        "0123456789abcdefghijklmnopqrstuvwxyz"
        "0123456789abcdefghijklmnopqrstuvwxyz"
        "0123456789abcdefghijklmnopqrstuvwxyz"
        "\r\n"
        "0\r\n"
        "\r\n";
    http::ResponseReader::RequireMore more = http::ResponseReader::REQUIRE_MORE;
    Optional<OTSError> err = rr.feed(more, MemPiece::from(response));
    TESTA_ASSERT(!err.present())
        (*err).issue();
    TESTA_ASSERT(more == http::ResponseReader::STOP).issue();
    deque<MemPiece> body;
    moveAssign(body, util::move(rr.mutableBody()));
    TESTA_ASSERT(pp::prettyPrint(body) == "[b\""
        "0123456789abcdefghijklmnopqrstuvwxyz"
        "0123456789abcdefghijklmnopqrstuvwxyz"
        "0123456789abcdefghijklmnopqrstuvwxyz"
        "0123456789abcdefghijklmnopqrstuvwxyz"
        "0123456789abcdefghijklmnopqrstuvwxyz"
        "\"]")
        (body).issue();
}
TESTA_DEF_JUNIT_LIKE1(ResponseReader_LongChunk_lowercase);

void ResponseReader_LongChunk_uppercase(const string&)
{
    auto_ptr<Logger> logger(createLogger("/", Logger::kDebug));
    Tracker tracker("tracker");
    http::ResponseReader rr(*logger, tracker);
    string response = "HTTP/1.1 200 OK\r\n"
        "Transfer-Encoding: chunked\r\n"
        "\r\n"
        "B4\r\n"
        "0123456789abcdefghijklmnopqrstuvwxyz"
        "0123456789abcdefghijklmnopqrstuvwxyz"
        "0123456789abcdefghijklmnopqrstuvwxyz"
        "0123456789abcdefghijklmnopqrstuvwxyz"
        "0123456789abcdefghijklmnopqrstuvwxyz"
        "\r\n"
        "0\r\n"
        "\r\n";
    http::ResponseReader::RequireMore more = http::ResponseReader::REQUIRE_MORE;
    Optional<OTSError> err = rr.feed(more, MemPiece::from(response));
    TESTA_ASSERT(!err.present())
        (*err).issue();
    TESTA_ASSERT(more == http::ResponseReader::STOP).issue();
    deque<MemPiece> body;
    moveAssign(body, util::move(rr.mutableBody()));
    TESTA_ASSERT(pp::prettyPrint(body) == "[b\""
        "0123456789abcdefghijklmnopqrstuvwxyz"
        "0123456789abcdefghijklmnopqrstuvwxyz"
        "0123456789abcdefghijklmnopqrstuvwxyz"
        "0123456789abcdefghijklmnopqrstuvwxyz"
        "0123456789abcdefghijklmnopqrstuvwxyz"
        "\"]")
        (body).issue();
}
TESTA_DEF_JUNIT_LIKE1(ResponseReader_LongChunk_uppercase);

} // namespace core
} // namespace tablestore
} // namespace aliyun

