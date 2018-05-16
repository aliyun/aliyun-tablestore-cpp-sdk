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
#include "plainbuffer/plain_buffer_builder.hpp"
#include "plainbuffer/plain_buffer_coded_stream.hpp"
#include "plainbuffer/plain_buffer_stream.hpp"
#include "plainbuffer/plain_buffer_crc8.hpp"
#include "src/tablestore/core/plainbuffer/reader.hpp"
#include "src/tablestore/core/plainbuffer/writer.hpp"
#include "tablestore/core/types.hpp"
#include "tablestore/util/random.hpp"
#include "tablestore/util/security.hpp"
#include "tablestore/util/optional.hpp"
#include "tablestore/util/mempiece.hpp"
#include "tablestore/util/foreach.hpp"
#include "testa/testa.hpp"
#include <tr1/functional>
#include <iostream>
#include <deque>
#include <string>
#include <memory>
#include <limits>
#include <numeric>
#include <cstdio>

using namespace std;
using namespace std::tr1;
using namespace std::tr1::placeholders;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

namespace {
void Crc8(const string&)
{
    for(int i = 0; i < 256; ++i) {
        uint8_t oracle = PlainBufferCrc8::CrcInt8(0, (uint8_t) i);
        uint8_t trial = 0;
        crc8(trial, (uint8_t) i);
        TESTA_ASSERT(oracle == trial)
            (oracle)
            (trial).issue();
    }
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(Crc8);

namespace {
void Crc8_U32(const string&)
{
    auto_ptr<random::Random> rng(random::newDefault(0));
    for(int i = 0; i < 10000; ++i) {
        uint32_t in = random::nextInt(*rng, numeric_limits<uint32_t>::max());
        uint8_t oracle = PlainBufferCrc8::CrcInt32(0, in);
        uint8_t trial = 0;
        crc8U32(trial, in);
        TESTA_ASSERT(trial == oracle)
            (oracle)
            (trial)
            (in).issue();
    }
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(Crc8_U32);

namespace {
uint64_t randomU64(random::Random& rng)
{
    uint64_t res = 0;
    for(int i = 0; i < 4; ++i) {
        res <<= 16;
        res |= random::nextInt(rng, numeric_limits<uint16_t>::max());
    }
    return res;
}

void Crc8_U64(const string&)
{
    auto_ptr<random::Random> rng(random::newDefault(0));
    for(int i = 0; i < 10000; ++i) {
        uint64_t in = randomU64(*rng);
        uint8_t oracle = PlainBufferCrc8::CrcInt64(0, in);
        uint8_t trial = 0;
        crc8U64(trial, in);
        TESTA_ASSERT(trial == oracle)
            (oracle)
            (trial)
            (in).issue();
    }
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(Crc8_U64);

namespace {
const string kAlphabet("abcdefgh.");

void randomStr(random::Random& rng, string& out, const string& alphabet, char terminator)
{
    for(;;) {
        char c = alphabet[random::nextInt(rng, alphabet.size())];
        if (c == terminator) {
            break;
        }
        out.push_back(c);
    }
}

void Crc8_Str(const string&)
{
    auto_ptr<random::Random> rng(random::newDefault());
    cout << "seed: " << rng->seed() << endl;
    for(int64_t i = 0; i < 10000; ++i) {
        string in;
        randomStr(*rng, in, kAlphabet, '.');
        uint8_t oracle = PlainBufferCrc8::CrcString(0, MemPiece::from(in));
        uint8_t trial = 0;
        crc8MemPiece(trial, MemPiece::from(in));
        TESTA_ASSERT(trial == oracle)
            (in)
            (oracle)
            (trial).issue();
    }
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(Crc8_Str);

namespace {

int64_t randomChoice(random::Random& rng, const deque<int64_t>& weights)
{
    int64_t total = accumulate(weights.begin(), weights.end(), 0);
    OTS_ASSERT(total > 0)(total);
    int64_t pick = random::nextInt(rng, total);
    for(int64_t i = 0, sz = weights.size(); i < sz; ++i) {
        if (pick < weights[i]) {
            return i;
        }
        pick -= weights[i];
    }
    OTS_ASSERT(false)
        (total)
        (pick);
    return 0;
}

void randomName(random::Random& rng, string& out)
{
    for(;;) {
        char c = kAlphabet[random::nextInt(rng, kAlphabet.size())];
        if (c != '.') {
            out.push_back(c);
            break;
        }
    }
    randomStr(rng, out, kAlphabet, '.');
}

void randomPrimaryKeyValue(random::Random& rng, PrimaryKeyValue& out)
{
    deque<PrimaryKeyType> types;
    types.push_back(kPKT_Integer);
    types.push_back(kPKT_String);
    types.push_back(kPKT_Binary);
    PrimaryKeyType tp = types[random::nextInt(rng, types.size())];
    switch(tp) {
    case kPKT_Integer:
        out.mutableInteger() = random::nextInt(rng, -32768, 32768);
        break;
    case kPKT_String:
        randomStr(rng, out.mutableStr(), kAlphabet, '.');
        break;
    case kPKT_Binary:
        randomStr(rng, out.mutableBlob(), kAlphabet, '.');
        break;
    }
}

void randomPrimaryKeyColumn(random::Random& rng, PrimaryKeyColumn& out)
{
    randomName(rng, out.mutableName());
    randomPrimaryKeyValue(rng, out.mutableValue());
}

void randomPrimaryKey(random::Random& rng, PrimaryKey& out)
{
    deque<int64_t> weights;
    weights.push_back(8);
    weights.push_back(4);
    weights.push_back(2);
    weights.push_back(1);
    int64_t len = randomChoice(rng, weights) + 1;
    for(int64_t i = 0; i < len; ++i) {
        randomPrimaryKeyColumn(rng, out.append());
    }
}

void randomAttrValue(random::Random& rng, AttributeValue& out)
{
    deque<AttributeValue::Category> types;
    types.push_back(AttributeValue::kString);
    types.push_back(AttributeValue::kInteger);
    types.push_back(AttributeValue::kBinary);
    types.push_back(AttributeValue::kBoolean);
    types.push_back(AttributeValue::kFloatPoint);
    AttributeValue::Category tp = types[random::nextInt(rng, types.size())];
    switch(tp) {
    case AttributeValue::kNone:
        OTS_ASSERT(false);
        break;
    case AttributeValue::kString:
        randomStr(rng, out.mutableStr(), kAlphabet, '.');
        break;
    case AttributeValue::kInteger:
        out.mutableInteger() = random::nextInt(rng, -32768, 32768);
        break;
    case AttributeValue::kBinary:
        randomStr(rng, out.mutableBlob(), kAlphabet, '.');
        break;
    case AttributeValue::kBoolean:
        out.mutableBoolean() = (random::nextInt(rng, 2) == 0);
        break;
    case AttributeValue::kFloatPoint:
        out.mutableFloatPoint() = random::nextInt(rng, -32768, 32768);
        break;
    }
}

void randomAttr(random::Random& rng, Attribute& out)
{
    randomName(rng, out.mutableName());
    randomAttrValue(rng, out.mutableValue());
    if (random::nextInt(rng, 2) == 0) {
        out.mutableTimestamp().reset(UtcTime::fromMsec(random::nextInt(rng, 987654321)));
    }
}

void randomAttrs(random::Random& rng, IVector<Attribute>& out)
{
    string lenStr;
    randomStr(rng, lenStr, "+++++.", '.');
    for(int64_t i = 0, sz = lenStr.size(); i < sz; ++i) {
        randomAttr(rng, out.append());
    }
}

void randomRow(random::Random& rng, Row& out)
{
    out.reset();
    randomPrimaryKey(rng, out.mutablePrimaryKey());
    randomAttrs(rng, out.mutableAttributes());
}

void to(RowPutChange& out, const Row& in)
{
    out.mutablePrimaryKey() = in.primaryKey();
    for(int64_t i = 0, sz = in.attributes().size(); i < sz; ++i) {
        out.mutableAttributes().append() = in.attributes()[i];
    }
}

void PlainBuffer_Deserialize_RandomRow(const string&)
{
    auto_ptr<random::Random> rng(random::newDefault());
    cout << "seed: " << rng->seed() << endl;
    for(int64_t repeat = 10000; repeat > 0; --repeat) {
        Row oracle;
        randomRow(*rng, oracle);

        RowPutChange chg;
        to(chg, oracle);
        string buf = PlainBufferBuilder::SerializeForRow(chg);

        Row trial;
        Optional<OTSError> err = plainbuffer::readRow(trial, MemPiece::from(buf));
        (void) err;
        TESTA_ASSERT(!err.present())
            (*err)
            (oracle).issue();
        TESTA_ASSERT(trial == oracle)
            (trial)
            (oracle)
            .issue();
    }
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(PlainBuffer_Deserialize_RandomRow);

namespace {
void PlainBuffer_Deserialize_Uint8(const string&)
{
    const uint8_t buf[] = {0x1};
    const uint8_t* b = buf;
    const uint8_t* e = b + sizeof(buf);
    uint8_t res = 0;
    Optional<OTSError> err = plainbuffer::impl::readUint8(res, b, e);
    TESTA_ASSERT(!err.present())(*err).issue();
    TESTA_ASSERT(res == 0x1)(res).issue();
    TESTA_ASSERT(b == e)
        (reinterpret_cast<uintptr_t>(b))
        (reinterpret_cast<uintptr_t>(e))
        .issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(PlainBuffer_Deserialize_Uint8);

namespace {
void PlainBuffer_Deserialize_Uint8_Error(const string&)
{
    const uint8_t buf[] = {};
    const uint8_t* b = buf;
    const uint8_t* e = b + sizeof(buf);
    uint8_t res = 0;
    Optional<OTSError> err = plainbuffer::impl::readUint8(res, b, e);
    TESTA_ASSERT(err.present()).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(PlainBuffer_Deserialize_Uint8_Error);

namespace {
void PlainBuffer_Deserialize_Uint32(const string&)
{
    const uint8_t buf[] = {0x1, 0x2, 0x3, 0x4};
    const uint8_t* b = buf;
    const uint8_t* e = b + sizeof(buf);
    uint32_t res = 0;
    Optional<OTSError> err = plainbuffer::impl::readUint32(res, b, e);
    TESTA_ASSERT(!err.present())(*err).issue();
    TESTA_ASSERT(res == 0x04030201)(res).issue();
    TESTA_ASSERT(b == e)
        (reinterpret_cast<uintptr_t>(b))
        (reinterpret_cast<uintptr_t>(e))
        .issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(PlainBuffer_Deserialize_Uint32);

namespace {
void PlainBuffer_Deserialize_Uint32_Error(const string&)
{
    const uint8_t buf[] = {0x1, 0x2, 0x3};
    const uint8_t* b = buf;
    const uint8_t* e = b + sizeof(buf);
    uint32_t res = 0;
    Optional<OTSError> err = plainbuffer::impl::readUint32(res, b, e);
    TESTA_ASSERT(err.present()).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(PlainBuffer_Deserialize_Uint32_Error);

namespace {
void PlainBuffer_Deserialize_Uint64(const string&)
{
    const uint8_t buf[] = {0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8};
    const uint8_t* b = buf;
    const uint8_t* e = b + sizeof(buf);
    uint64_t res = 0;
    Optional<OTSError> err = plainbuffer::impl::readUint64(res, b, e);
    TESTA_ASSERT(!err.present())(*err).issue();
    TESTA_ASSERT(res == 0x0807060504030201)(res).issue();
    TESTA_ASSERT(b == e)
        (reinterpret_cast<uintptr_t>(b))
        (reinterpret_cast<uintptr_t>(e))
        .issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(PlainBuffer_Deserialize_Uint64);

namespace {
void PlainBuffer_Deserialize_Uint64_Error(const string&)
{
    const uint8_t buf[] = {0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7};
    const uint8_t* b = buf;
    const uint8_t* e = b + sizeof(buf);
    uint64_t res = 0;
    Optional<OTSError> err = plainbuffer::impl::readUint64(res, b, e);
    TESTA_ASSERT(err.present()).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(PlainBuffer_Deserialize_Uint64_Error);

namespace {
void PlainBuffer_Deserialize_Header(const string&)
{
    const uint8_t buf[] = {0x75, 0, 0, 0};
    const uint8_t* b = buf;
    const uint8_t* e = b + sizeof(buf);
    Optional<OTSError> err = plainbuffer::impl::readHeader(b, e);
    TESTA_ASSERT(!err.present())(*err).issue();
    TESTA_ASSERT(b == e)
        (reinterpret_cast<uintptr_t>(b))
        (reinterpret_cast<uintptr_t>(e))
        .issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(PlainBuffer_Deserialize_Header);

namespace {
void PlainBuffer_Deserialize_Header_Error(const string&)
{
    const uint8_t buf[] = {0x75, 0x1, 0x2, 0x3};
    const uint8_t* b = buf;
    const uint8_t* e = b + sizeof(buf);
    Optional<OTSError> err = plainbuffer::impl::readHeader(b, e);
    TESTA_ASSERT(err.present()).issue();
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(PlainBuffer_Deserialize_Header_Error);

namespace {
void PlainBuffer_Serialize_RandomAttributeValue(const string&)
{
    auto_ptr<random::Random> rng(random::newDefault());
    cout << "seed: " << rng->seed() << endl;
    for(int64_t repeat = 10000; repeat > 0; --repeat) {
        AttributeValue val;
        randomAttrValue(*rng, val);

        string oracle = PlainBufferBuilder::SerializeColumnValue(val);
        string trial;
        plainbuffer::write(trial, val);

        TESTA_ASSERT(trial == oracle)
            (MemPiece::from(oracle))
            (MemPiece::from(trial))
            .issue();
    }
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(PlainBuffer_Serialize_RandomAttributeValue);

namespace {
void PlainBuffer_Serialize_RandomPrimaryKeyValue(const string&)
{
    auto_ptr<random::Random> rng(random::newDefault());
    cout << "seed: " << rng->seed() << endl;
    for(int64_t repeat = 10000; repeat > 0; --repeat) {
        PrimaryKeyValue val;
        randomPrimaryKeyValue(*rng, val);

        string oracle = PlainBufferBuilder::SerializePrimaryKeyValue(val);
        string trial;
        plainbuffer::write(trial, val);
        
        TESTA_ASSERT(trial == oracle)
            (MemPiece::from(oracle))
            (MemPiece::from(trial))
            .issue();
    }
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(PlainBuffer_Serialize_RandomPrimaryKeyValue);

namespace {

bool testRowPutChange(const Row& row)
{
    RowPutChange chg;
    to(chg, row);
    string oracle = PlainBufferBuilder::SerializeForRow(chg);
    string trial;
    plainbuffer::write(trial, chg);

    if (trial == oracle) {
        return true;
    } else {
        cerr << "oracle: " << pp::prettyPrint(MemPiece::from(oracle)) << endl;
        cerr << "trial:  " << pp::prettyPrint(MemPiece::from(trial)) << endl;
        return false;
    }
}

Row shrinkPkey(const Row& in, const function<bool(const Row&)>& test)
{
    Row shrinked;
    for(int64_t i = 0; i < in.attributes().size(); ++i) {
        shrinked.mutableAttributes().append() = in.attributes()[i];
    }
    for(int64_t i = 0; i < in.primaryKey().size(); ++i) {
        shrinked.mutablePrimaryKey().reset();
        for(int64_t j = 0; j < in.primaryKey().size(); ++j) {
            if (j == i) {
                continue;
            }
            shrinked.mutablePrimaryKey().append() = in.primaryKey()[j];
        }
        if (!test(shrinked)) {
            return shrinked;
        }
    }
    return in;
}

Row shrinkAttrs(const Row& in, const function<bool(const Row&)>& test)
{
    Row shrinked;
    for(int64_t i = 0; i < in.primaryKey().size(); ++i) {
        shrinked.mutablePrimaryKey().append() = in.primaryKey()[i];
    }
    for(int64_t i = 0; i < in.attributes().size(); ++i) {
        shrinked.mutableAttributes().reset();
        for(int64_t j = 0; j < in.attributes().size(); ++j) {
            if (j == i) {
                continue;
            }
            shrinked.mutableAttributes().append() = in.attributes()[j];
        }
        if (!test(shrinked)) {
            return shrinked;
        }
    }
    return in;
}

template<typename T>
T shrinkTrampoline(
    const T& in,
    const function<bool(const T&)>& test,
    const function<T(const T&, const function<bool(const T&)>&)>& shrinkFn)
{
    T original = in;
    for(;;) {
        T shrinked = shrinkFn(original, test);
        if (shrinked == original) {
            return shrinked;
        }
        moveAssign(original, util::move(shrinked));
    }
    OTS_ASSERT(false);
    return in;
}

void shrink(const Row& row, const function<bool(const Row&)>& test)
{
    Row shrinked = shrinkTrampoline<Row>(row, test, shrinkPkey);
    shrinked = shrinkTrampoline<Row>(shrinked, test, shrinkAttrs);
    TESTA_ASSERT(false)
        (shrinked)
        .issue();
}

void PlainBuffer_Serialize_RandomRowPutChange(const string&)
{
    auto_ptr<random::Random> rng(random::newDefault());
    cout << "seed: " << rng->seed() << endl;
    for(int64_t repeat = 10000; repeat > 0; --repeat) {
        Row row;
        randomRow(*rng, row);

        if (!testRowPutChange(row)) {
            shrink(row, testRowPutChange);
        }
    }
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(PlainBuffer_Serialize_RandomRowPutChange);

namespace {
void to(RowDeleteChange& out, const PrimaryKey& in)
{
    out.mutablePrimaryKey() = in;
}

bool testRowDeleteChange(const PrimaryKey& pkey)
{
    RowDeleteChange chg;
    to(chg, pkey);
    string oracle = PlainBufferBuilder::SerializeForRow(chg);
    string trial;
    plainbuffer::write(trial, chg);

    if (trial == oracle) {
        return true;
    } else {
        cerr << "oracle: " << pp::prettyPrint(MemPiece::from(oracle)) << endl;
        cerr << "trial:  " << pp::prettyPrint(MemPiece::from(trial)) << endl;
        return false;
    }
}

void PlainBuffer_Serialize_RandomRowDeleteChange(const string&)
{
    auto_ptr<random::Random> rng(random::newDefault());
    cout << "seed: " << rng->seed() << endl;
    for(int64_t repeat = 10000; repeat > 0; --repeat) {
        PrimaryKey pkey;
        randomPrimaryKey(*rng, pkey);

        TESTA_ASSERT(testRowDeleteChange(pkey))
            (pkey)
            .issue();
    }
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(PlainBuffer_Serialize_RandomRowDeleteChange);

namespace {
bool testRowUpdateChange(const RowUpdateChange& chg)
{
    string oracle = PlainBufferBuilder::SerializeForRow(chg);
    string trial;
    plainbuffer::write(trial, chg);

    if (trial == oracle) {
        return true;
    } else {
        cerr << "oracle: " << pp::prettyPrint(MemPiece::from(oracle)) << endl;
        cerr << "trial:  " << pp::prettyPrint(MemPiece::from(trial)) << endl;
        return false;
    }
}

void randomPutCell(random::Random& rng, RowUpdateChange::Update& out)
{
    out.mutableType() = RowUpdateChange::Update::kPut;
    randomName(rng, out.mutableAttrName());
    AttributeValue val;
    randomAttrValue(rng, val);
    out.mutableAttrValue().reset(util::move(val));
    if (random::nextInt(rng, 2) == 0) {
        int64_t tm = random::nextInt(rng, 987654321);
        out.mutableTimestamp().reset(UtcTime::fromMsec(tm));
    }
}

void randomDelCell(random::Random& rng, RowUpdateChange::Update& out)
{
    out.mutableType() = RowUpdateChange::Update::kDelete;
    randomName(rng, out.mutableAttrName());
    if (random::nextInt(rng, 2) == 0) {
        int64_t tm = random::nextInt(rng, 987654321);
        out.mutableTimestamp().reset(UtcTime::fromMsec(tm));
    }
}

void randomDelCol(random::Random& rng, RowUpdateChange::Update& out)
{
    out.mutableType() = RowUpdateChange::Update::kDeleteAll;
    randomName(rng, out.mutableAttrName());
}

void randomRowUpdateChange(random::Random& rng, RowUpdateChange& out)
{
    randomPrimaryKey(rng, out.mutablePrimaryKey());
    string pat;
    randomStr(rng, pat, "PDC.", '.');
    FOREACH_ITER(i, pat) {
        char c = *i;
        switch(c) {
        case 'P':
            randomPutCell(rng, out.mutableUpdates().append());
            break;
        case 'D':
            randomDelCell(rng, out.mutableUpdates().append());
            break;
        case 'C':
            randomDelCol(rng, out.mutableUpdates().append());
            break;
        default:
            OTS_ASSERT(false)(c);
        }
    }
}

RowUpdateChange shrinkPkeyInRowUpdateChange(
    const RowUpdateChange& in,
    const function<bool(const RowUpdateChange&)>& test)
{
    RowUpdateChange shrinked = in;
    for(int64_t i = 0; i < in.primaryKey().size(); ++i) {
        shrinked.mutablePrimaryKey().reset();
        for(int64_t j = 0; j < in.primaryKey().size(); ++j) {
            if (j == i) {
                continue;
            }
            shrinked.mutablePrimaryKey().append() = in.primaryKey()[j];
        }
        if (!test(shrinked)) {
            return shrinked;
        }
    }
    return in;
}

RowUpdateChange shrinkUpdates(
    const RowUpdateChange& in,
    const function<bool(const RowUpdateChange&)>& test)
{
    RowUpdateChange shrinked = in;
    for(int64_t i = 0; i < in.updates().size(); ++i) {
        shrinked.mutableUpdates().reset();
        for(int64_t j = 0; j < in.updates().size(); ++j) {
            if (j == i) {
                continue;
            }
            shrinked.mutableUpdates().append() = in.updates()[j];
        }
        if (!test(shrinked)) {
            return shrinked;
        }
    }
    return in;
}

void shrink(
    const RowUpdateChange& row,
    const function<bool(const RowUpdateChange&)>& test)
{
    RowUpdateChange shrinked = shrinkTrampoline<RowUpdateChange>(
        row, test, shrinkPkeyInRowUpdateChange);
    shrinked = shrinkTrampoline<RowUpdateChange>(shrinked, test, shrinkUpdates);
    TESTA_ASSERT(false)
        (shrinked)
        .issue();
}

void PlainBuffer_Serialize_RandomRowUpdateChange(const string&)
{
    auto_ptr<random::Random> rng(random::newDefault());
    cout << "seed: " << rng->seed() << endl;
    for(int64_t repeat = 10000; repeat > 0; --repeat) {
        RowUpdateChange chg;
        randomRowUpdateChange(*rng, chg);

        if(!testRowUpdateChange(chg)) {
            shrink(chg, testRowUpdateChange);
        }
    }
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(PlainBuffer_Serialize_RandomRowUpdateChange);

namespace {
void PlainBuffer_Serialize_RandomPrimaryKey(const string&)
{
    auto_ptr<random::Random> rng(random::newDefault());
    cout << "seed: " << rng->seed() << endl;
    for(int64_t repeat = 10000; repeat > 0; --repeat) {
        PrimaryKey pk;
        randomPrimaryKey(*rng, pk);

        string oracle = PlainBufferBuilder::SerializePrimaryKey(pk);
        string _trial;
        plainbuffer::write(_trial, pk);

        TESTA_ASSERT(_trial == oracle)
            (MemPiece::from(oracle))
            (MemPiece::from(_trial))
            .issue();
    }
}
} // namespace
TESTA_DEF_JUNIT_LIKE1(PlainBuffer_Serialize_RandomPrimaryKey);

} // namespace core
} // namespace tablestore
} // namespace aliyun

