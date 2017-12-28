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
#include "response_reader.hpp"
#include "types.hpp"
#include "tablestore/core/types.hpp"
#include "tablestore/util/logging.hpp"
#include "tablestore/util/try.hpp"

using namespace std;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {
namespace http {

namespace impl {

class ResponseParserState
{
public:
    virtual ~ResponseParserState() {}

    virtual ResponseParserState* parse(Optional<OTSError>*, uint8_t) =0;
};

} // namespace impl

namespace {

class StatusLineParser: public impl::ResponseParserState
{
public:
    explicit StatusLineParser(
        Logger&,
        const Tracker&,
        BookmarkInputStream&,
        int64_t&,
        impl::ResponseParserState*);

    impl::ResponseParserState* parse(Optional<OTSError>*, uint8_t);

private:
    enum State
    {
        INIT,
        OTHERS,
        CR,
        LF,
    };

private:
    Logger& mLogger;
    const Tracker& mTracker;
    State mState;
    BookmarkInputStream& mInputStream;
    int64_t& mHttpStatus;
    impl::ResponseParserState* mHeaderParser;
};

StatusLineParser::StatusLineParser(
    Logger& logger,
    const Tracker& tracker,
    BookmarkInputStream& is,
    int64_t& httpStatus,
    impl::ResponseParserState* headerParser)
  : mLogger(logger),
    mTracker(tracker),
    mState(INIT),
    mInputStream(is),
    mHttpStatus(httpStatus),
    mHeaderParser(headerParser)
{}

namespace {

inline bool digit(uint8_t c)
{
    return '0' <= c && c <= '9';
}

inline bool blank(uint8_t c)
{
    return c == ' ';
}

void fillError(Optional<OTSError>& err, const string& msg)
{
    OTSError e(OTSError::kPredefined_CorruptedResponse);
    e.mutableMessage() = msg;
    err.reset(util::move(e));
}

} // namespace

impl::ResponseParserState* StatusLineParser::parse(Optional<OTSError>* err, uint8_t byte)
{
    switch(mState) {
    case INIT: {
        mInputStream.pushBookmark();
        if (byte == '\r') {
            mState = CR;
        } else {
            mState = OTHERS;
        }
        break;
    }
    case OTHERS: {
        if (byte == '\r') {
            mState = CR;
        }
        break;
    }
    case CR: {
        if (byte == '\n') {
            mState = LF;
        } else {
            OTS_LOG_ERROR(mLogger)
                ("Tracker", mTracker)
                .what("HTTP response is corrupted: '\r' is not followed by a '\n'.");
            fillError(*err, "HTTP response is corrupted: '\r' is not followed by a '\n'.");
            return NULL;
        }
        break;
    }
    case LF: {
        OTS_ASSERT(false)(mTracker);
        break;
    }
    }

    if (mState != LF) {
        return this;
    }

    deque<MemPiece> pieces;
    mInputStream.popBookmark(pieces);
    if (pieces.size() != 1) {
        OTS_LOG_ERROR(mLogger)
            ("Tracker", mTracker)
            ("PiecesSize", pieces.size())
            ("PiecesLength", totalLength(pieces))
            .what("Too long status line");
        fillError(*err, "HTTP response is corrupted: too long status line.");
        return NULL;
    }
    MemPiece statusLine = pieces.front();
    const MemPiece http11 = MemPiece::from("HTTP/1.1 ");
    if (!statusLine.startsWith(http11)) {
        OTS_LOG_ERROR(mLogger)
            ("Tracker", mTracker)
            ("StatusLine", statusLine)
            .what("Status line syntax is incorrect.");
        fillError(*err, "HTTP response is corrupted: status line syntax is incorrect.");
        return NULL;
    }
    MemPiece sub = statusLine.subpiece(http11.length());
    const uint8_t* b = sub.data();
    const uint8_t* e = b + sub.length();
    const uint8_t* c = b;
    for(; c < e && digit(*c); ++c) {
    }
    MemPiece status = MemPiece(b, c - b);
    Optional<string> ec = status.to<int64_t>(mHttpStatus);
    if (ec.present()) {
        OTS_LOG_ERROR(mLogger)
            ("Tracker", mTracker)
            ("StatusLine", statusLine)
            ("ErrorMessage", *ec)
            .what("Status line syntax is incorrect.");
        fillError(*err, "HTTP response is corrupted: status line syntax is incorrect.");
        return NULL;
    }
    return mHeaderParser;
}

class ContentLengthParser: public impl::ResponseParserState
{
public:
    explicit ContentLengthParser(
        Logger&,
        const Tracker&,
        BookmarkInputStream&,
        deque<MemPiece>& body);

    impl::ResponseParserState* parse(Optional<OTSError>*, uint8_t);

    int64_t* mutableRemaingBytes()
    {
        return &mRemainingBytes;
    }

private:
    enum State
    {
        INIT,
        COUNTING,
    };

private:
    Logger& mLogger;
    const Tracker& mTracker;
    State mState;
    BookmarkInputStream& mInputStream;
    deque<MemPiece>& mBody;
    int64_t mRemainingBytes;
};

ContentLengthParser::ContentLengthParser(
    Logger& logger,
    const Tracker& tracker,
    BookmarkInputStream& is,
    deque<MemPiece>& body)
  : mLogger(logger),
    mTracker(tracker),
    mState(INIT),
    mInputStream(is),
    mBody(body),
    mRemainingBytes(0)
{}

impl::ResponseParserState* ContentLengthParser::parse(Optional<OTSError>* err, uint8_t byte)
{
    switch(mState) {
    case INIT: {
        mInputStream.pushBookmark();
        OTS_LOG_DEBUG(mLogger)
            ("RemainingBytes", mRemainingBytes);
        mState = COUNTING;
        --mRemainingBytes;
        break;
    }
    case COUNTING: {
        --mRemainingBytes;
        break;
    }
    }
    OTS_ASSERT(mRemainingBytes >= 0)
        (mTracker)
        (mRemainingBytes);

    if (mRemainingBytes == 0) {
        mInputStream.popBookmark(mBody);
        return NULL;
    } else {
        return this;
    }
}

class HeaderParser: public impl::ResponseParserState
{
public:
    explicit HeaderParser(
        Logger&,
        const Tracker&,
        BookmarkInputStream&,
        InplaceHeaders&,
        ContentLengthParser*,
        impl::ResponseParserState*);

    impl::ResponseParserState* parse(Optional<OTSError>*, uint8_t);

private:
    enum State
    {
        /**
         *                  OTHERS-+
         *                  ^      |\r
         *        \n     \r |      v
         *   LF1<---CR1<---INIT    CR0
         *                  ^      |
         *                  |  \n  |
         *                  +------+
         */
        INIT,
        OTHERS,
        CR0,
        CR1,
        LF1,
    };

private:
    Logger& mLogger;
    const Tracker& mTracker;
    State mState;
    BookmarkInputStream& mInputStream;
    InplaceHeaders& mHeaders;
    ContentLengthParser* mContentLengthParser;
    impl::ResponseParserState* mChunkedBodyParser;
};

HeaderParser::HeaderParser(
    Logger& logger,
    const Tracker& tracker,
    BookmarkInputStream& is,
    InplaceHeaders& headers,
    ContentLengthParser* clparser,
    impl::ResponseParserState* cbparser)
  : mLogger(logger),
    mTracker(tracker),
    mState(INIT),
    mInputStream(is),
    mHeaders(headers),
    mContentLengthParser(clparser),
    mChunkedBodyParser(cbparser)
{}

impl::ResponseParserState* HeaderParser::parse(Optional<OTSError>* err, uint8_t byte)
{
    switch(mState) {
    case INIT: {
        mInputStream.pushBookmark();
        if (byte == '\r') {
            mState = CR1;
        } else {
            mState = OTHERS;
        }
        break;
    }
    case OTHERS: {
        if (byte == '\r') {
            mState = CR0;
        } else {
            mState = OTHERS;
        }
        break;
    }
    case CR0: {
        if (byte == '\n') {
            mState = INIT;
        } else {
            OTS_LOG_ERROR(mLogger)
                ("Tracker", mTracker)
                .what("HTTP response is corrupted: '\r' is not followed by a '\n'.");
            fillError(*err, "HTTP response is corrupted: '\r' is not followed by a '\n'.");
            return NULL;
        }
        break;
    }
    case CR1: {
        if (byte == '\n') {
            mState = LF1;
        } else {
            OTS_LOG_ERROR(mLogger)
                ("Tracker", mTracker)
                .what("HTTP response is corrupted: '\r' is not followed by a '\n'.");
            fillError(*err, "HTTP response is corrupted: '\r' is not followed by a '\n'.");
            return NULL;
        }
        break;
    }
    case LF1: {
        OTS_ASSERT(false)(mTracker);
    }
    }

    if (mState != INIT) {
        if (mState == LF1) {
            InplaceHeaders::const_iterator it = mHeaders.find(MemPiece::from("Transfer-Encoding"));
            if (it != mHeaders.end()) {
                MemPiece te = it->second;
                if (te != MemPiece::from("chunked")) {
                    OTS_LOG_ERROR(mLogger)
                        ("Tracker", mTracker)
                        ("TransferEncoding", te.to<string>())
                        .what("Unrecognize Transfer-Encoding.");
                    fillError(*err, "HTTP response is corrupted: Unrecognize Transfer-Encoding " + te.to<string>());
                    return NULL;
                }
                return mChunkedBodyParser;
            }
            it = mHeaders.find(MemPiece::from("Content-Length"));
            if (it != mHeaders.end()) {
                int64_t contentLength = 0;
                Optional<string> emsg = it->second.to<int64_t>(contentLength);
                if (emsg.present()) {
                    OTS_LOG_ERROR(mLogger)
                        ("Tracker", mTracker)
                        ("ErrorMessage", *emsg)
                        .what("Neither Content-Length nor chunked Transfer-Encoding is present.");
                    fillError(*err, "HTTP response is corrupted: " + *emsg);
                    return NULL;
                }
                if (contentLength == 0) {
                    return NULL;
                } else {
                    *mContentLengthParser->mutableRemaingBytes() = contentLength;
                    return mContentLengthParser;
                }
            }
            OTS_LOG_ERROR(mLogger)
                ("Tracker", mTracker)
                .what("Neither Content-Length nor chunked Transfer-Encoding is present.");
            fillError(*err, "HTTP response is corrupted: "
                "neither Content-Length nor chunked Transfer-Encoding is present.");
            return NULL;
        } else {
            return this;
        }
    }

    deque<MemPiece> pieces;
    mInputStream.popBookmark(pieces);
    if (pieces.size() != 1) {
        OTS_LOG_ERROR(mLogger)
            ("Tracker", mTracker)
            ("PiecesSize", pieces.size())
            ("PiecesLength", totalLength(pieces))
            .what("HTTP response is corrupted: too long header line.");
        fillError(*err, "HTTP response is corrupted: too long header line.");
        return NULL;
    }
    MemPiece field = pieces.front();
    const uint8_t* b = field.data();
    const uint8_t* e = b + field.length();
    const uint8_t* c = b;
    for(; c < e && *c != ':'; ++c) {
    }
    if (c == e) {
        OTS_LOG_ERROR(mLogger)
            ("Tracker", mTracker)
            ("Field", field.to<string>())
            .what("HTTP response is corrupted: missing ':'.");
        fillError(*err, "HTTP response is corrupted: missing ':'.");
        return NULL;
    }
    MemPiece fieldName = MemPiece(b, c - b);
    for(++c; c < e && blank(*c); ++c) {
    }
    for(; c < e && (blank(*(e-1)) || *(e-1) == '\r' || *(e-1) == '\n'); --e) {
    }
    if (c == e) {
        OTS_LOG_ERROR(mLogger)
            ("Tracker", mTracker)
            ("Field", field.to<string>())
            .what("HTTP response is corrupted: missing header field.");
        fillError(*err, "HTTP response is corrupted: missing header field.");
        return NULL;
    }
    MemPiece fieldValue = MemPiece(c, e - c);
    bool ret = mHeaders.insert(make_pair(fieldName, fieldValue)).second;
    if (!ret) {
        OTS_LOG_ERROR(mLogger)
            ("Tracker", mTracker)
            ("Field", field.to<string>())
            .what("HTTP response is corrupted: duplicated header field.");
        fillError(*err, "HTTP response is corrupted: duplicated header field.");
        return NULL;
    }

    return this;
}

class ChunkedBodyParser: public impl::ResponseParserState
{
public:
    explicit ChunkedBodyParser(
        Logger&,
        const Tracker&,
        BookmarkInputStream&,
        deque<MemPiece>& body);

    impl::ResponseParserState* parse(Optional<OTSError>*, uint8_t);

private:
    enum State
    {
        /**
         * +-->INIT
         * |    |
         * |    v
         * |   CHUNK_SIZE
         * |    |\r
         * |    v            size=0,\n
         * |   CHUNK_SIZE_CR----------->BODY_END_LF0
         * |    |\n                      |\r
         * |    v                        v
         * |   CHUNK                    BODY_END_CR1
         * |    |\r                      |\n
         * |\n  v                        v
         * +---CHUNK_CR                 BODY_END
         */
        INIT,
        CHUNK_SIZE,
        CHUNK_SIZE_CR,
        CHUNK,
        CHUNK_CR,
        BODY_END_LF0,
        BODY_END_CR1,
        BODY_END,
    };

private:
    Logger& mLogger;
    const Tracker& mTracker;
    BookmarkInputStream& mInputStream;
    State mState;
    deque<MemPiece>& mBody;
    int64_t mExpectChunkSize;
    int64_t mRealChunkSize;
};

ChunkedBodyParser::ChunkedBodyParser(
    Logger& logger,
    const Tracker& tracker,
    BookmarkInputStream& is,
    deque<MemPiece>& body)
  : mLogger(logger),
    mTracker(tracker),
    mInputStream(is),
    mState(INIT),
    mBody(body),
    mExpectChunkSize(0),
    mRealChunkSize(0)
{}

impl::ResponseParserState* ChunkedBodyParser::parse(Optional<OTSError>* err, uint8_t byte)
{
    switch(mState) {
    case INIT: {
        mRealChunkSize = 0;
        if ('0' <= byte && byte <= '9') {
            mState = CHUNK_SIZE;
            mExpectChunkSize = (byte - '0');
        } else if ('a' <= byte && byte <= 'f') {
            mState = CHUNK_SIZE;
            mExpectChunkSize = (byte - 'a') + 10;
        } else if ('A' <= byte && byte <= 'F') {
            mState = CHUNK_SIZE;
            mExpectChunkSize = (byte - 'A') + 10;
        } else {
            OTS_LOG_ERROR(mLogger)
                ("Tracker", mTracker)
                ("Byte", byte)
                .what("HTTP response is corrupted: chunk size is required.");
            fillError(*err, "HTTP response is corrupted: chunk size is required.");
            return NULL;
        }
        break;
    }
    case CHUNK_SIZE: {
        if (byte == '\r') {
            mState = CHUNK_SIZE_CR;
        } else if ('0' <= byte && byte <= '9') {
            mExpectChunkSize *= 16;
            mExpectChunkSize += (byte - '0');
        } else if ('a' <= byte && byte <= 'f') {
            mExpectChunkSize *= 16;
            mExpectChunkSize += (byte - 'a') + 10;
        } else if ('A' <= byte && byte <= 'F') {
            mExpectChunkSize *= 16;
            mExpectChunkSize += (byte - 'A') + 10;
        } else {
            OTS_LOG_ERROR(mLogger)
                ("Tracker", mTracker)
                ("Byte", byte)
                .what("HTTP response is corrupted: chunk size has syntax error.");
            fillError(*err, "HTTP response is corrupted: chunk size has syntax error.");
            return NULL;
        }
        if (mExpectChunkSize < 0) {
            OTS_LOG_ERROR(mLogger)
                ("Tracker", mTracker)
                ("ChunkSize", mExpectChunkSize)
                .what("HTTP response is corrupted: chunked size overflows.");
            fillError(*err, "HTTP response is corrupted: chunked size overflows.");
            return NULL;
        }
        break;
    }
    case CHUNK_SIZE_CR: {
        if (byte == '\n') {
            if (mExpectChunkSize > 0) {
                OTS_LOG_DEBUG(mLogger)
                    ("Tracker", mTracker)
                    ("ChunkSize", mExpectChunkSize)
                    .what("a new chunk is on the way.");
                mState = CHUNK;
            } else {
                mState = BODY_END_LF0;
            }
        } else {
            OTS_LOG_ERROR(mLogger)
                ("Tracker", mTracker)
                .what("HTTP response is corrupted: '\r' is not followed by a '\n'.");
            fillError(*err, "HTTP response is corrupted: '\r' is not followed by a '\n'.");
            return NULL;
        }
        break;
    }
    case CHUNK: {
        OTS_ASSERT(mRealChunkSize <= mExpectChunkSize)
            (mRealChunkSize)
            (mExpectChunkSize);
        if (mRealChunkSize < mExpectChunkSize) {
            if (mRealChunkSize == 0) {
                mInputStream.pushBookmark();
            }
            ++mRealChunkSize;
            if (mRealChunkSize == mExpectChunkSize) {
                mInputStream.popBookmark(mBody);
                OTS_LOG_DEBUG(mLogger)
                    ("RealChunkSize", mRealChunkSize)
                    .what("a chunk received.");
            }
        } else if (byte == '\r') {
            mState = CHUNK_CR;
        } else {
            OTS_LOG_ERROR(mLogger)
                ("Tracker", mTracker)
                ("Byte", byte)
                .what("HTTP response is corrupted: chunked body must be followed by a CRLF .");
            fillError(*err, "HTTP response is corrupted: chunked body must be followed by a CRLF.");
            return NULL;
        }
        break;
    }
    case CHUNK_CR: {
        if (byte == '\n') {
            mState = INIT;
        } else {
            OTS_LOG_ERROR(mLogger)
                ("Tracker", mTracker)
                .what("HTTP response is corrupted: '\r' is not followed by a '\n'.");
            fillError(*err, "HTTP response is corrupted: '\r' is not followed by a '\n'.");
        }
        break;
    }
    case BODY_END_LF0: {
        if (byte == '\r') {
            mState = BODY_END_CR1;
        } else {
            OTS_LOG_ERROR(mLogger)
                ("Tracker", mTracker)
                .what("HTTP response is corrupted: a tailing blank line is required in chunked body.");
            fillError(*err, "HTTP response is corrupted: a tailing blank line is required in chunked body.");
        }
        break;
    }
    case BODY_END_CR1: {
        if (byte == '\n') {
            mState = BODY_END;
        } else {
            OTS_LOG_ERROR(mLogger)
                ("Tracker", mTracker)
                .what("HTTP response is corrupted: '\r' is not followed by a '\n'.");
            fillError(*err, "HTTP response is corrupted: '\r' is not followed by a '\n'.");
        }
        break;
    }
    case BODY_END: {
        OTS_ASSERT(false);
    }
    }

    if (mState == BODY_END) {
        return NULL;
    } else {
        return this;
    }
}

} // namespace

ResponseReader::ResponseReader(
    Logger& logger,
    const Tracker& tracker)
  : mLogger(logger),
    mTracker(tracker),
    mInputStream(tracker),
    mHttpStatus(0)
{
    ContentLengthParser* contentLengthParser = new ContentLengthParser(
        mLogger, mTracker, mInputStream, mBody);
    ChunkedBodyParser* chunkedBodyParser = new ChunkedBodyParser(
        mLogger, mTracker, mInputStream, mBody);
    HeaderParser* headerParser = new HeaderParser(
        mLogger, mTracker, mInputStream, mHeaders,
        contentLengthParser, chunkedBodyParser);
    StatusLineParser* statusLineParser = new StatusLineParser(
        mLogger, mTracker, mInputStream, mHttpStatus, headerParser);
    mStates.push_back(statusLineParser);
    mStates.push_back(headerParser);
    mStates.push_back(contentLengthParser);
    mStates.push_back(chunkedBodyParser);
    mState = statusLineParser;
}

ResponseReader::~ResponseReader()
{
    for(; !mStates.empty(); mStates.pop_back()) {
        delete mStates.back();
    }
}

Optional<OTSError> ResponseReader::feed(RequireMore& more, const MemPiece& piece)
{
    OTS_ASSERT(piece.length() > 0);
    mInputStream.feed(piece);
    TRY(parse(more));
    return Optional<OTSError>();
}

Optional<OTSError> ResponseReader::parse(RequireMore& more)
{
    OTS_ASSERT(mState != NULL);
    more = REQUIRE_MORE;
    for(;;) {
        Optional<uint8_t> ch = mInputStream.peek();
        OTS_ASSERT(ch.present());

        Optional<OTSError> err;
        mState = mState->parse(&err, *ch);
        TRY(err);
        if (mState == NULL) {
            more = STOP;
            break;
        }

        bool moreBytes = mInputStream.moveNext();
        if (!moreBytes) {
            break;
        }
    }
    return Optional<OTSError>();
}

} // namespace http
} // namespace core
} // namespace tablestore
} // namespace aliyun

