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
#include "tablestore/core/types.hpp"
#include "tablestore/util/prettyprint.hpp"
#include "tablestore/util/foreach.hpp"
#include "testa/testa.hpp"
#include <tr1/functional>
#include <tr1/tuple>
#include <string>
#include <deque>
#include <map>

using namespace std;
using namespace std::tr1;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

void Action_pp(const string&)
{
    deque<Action> acts;
    collectActions(&acts);
    FOREACH_ITER(i, acts) {
        FOREACH_ITER(j, acts) {
            if (i != j) {
                TESTA_ASSERT(pp::prettyPrint(*i) != pp::prettyPrint(*j))
                    (*i)(*j).issue();
            }
        }
    }
}
TESTA_DEF_JUNIT_LIKE1(Action_pp);

void PrimaryKeyType_pp(const string&)
{
    deque<PrimaryKeyType> tps;
    collectPrimaryKeyTypes(&tps);
    FOREACH_ITER(i, tps) {
        FOREACH_ITER(j, tps) {
            if (i != j) {
                TESTA_ASSERT(pp::prettyPrint(*i) != pp::prettyPrint(*j))
                    (*i)(*j).issue();
            }
        }
    }
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKeyType_pp);

void PrimaryKeyOption_pp(const string&)
{
    deque<PrimaryKeyOption> opts;
    collectPrimaryKeyOptions(&opts);
    FOREACH_ITER(i, opts) {
        FOREACH_ITER(j, opts) {
            if (i != j) {
                TESTA_ASSERT(pp::prettyPrint(*i) != pp::prettyPrint(*j))
                    (*i)(*j).issue();
            }
        }
    }
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKeyOption_pp);

void BloomFilterType_pp(const string&)
{
    deque<BloomFilterType> xs;
    collectBloomFilterType(&xs);
    FOREACH_ITER(i, xs) {
        FOREACH_ITER(j, xs) {
            if (i != j) {
                TESTA_ASSERT(pp::prettyPrint(*i) != pp::prettyPrint(*j))
                    (*i)(*j).issue();
            }
        }
    }
}
TESTA_DEF_JUNIT_LIKE1(BloomFilterType_pp);

void DequeBasedVector_move(const string&)
{
    DequeBasedVector<int> xs;
    xs.append() = 1;
    DequeBasedVector<int> ys;
    ys.append() = 2;
    util::moveAssign(&xs, util::move(ys));

    TESTA_ASSERT(pp::prettyPrint(xs) == "[2]")
        (xs)(ys).issue();
    TESTA_ASSERT(pp::prettyPrint(ys) == "[]")
        (xs)(ys).issue();
}
TESTA_DEF_JUNIT_LIKE1(DequeBasedVector_move);

void PrimaryKeyColumnSchema_pp(const string&)
{
    PrimaryKeyColumnSchema s;
    *s.mutableName() = "pkey";
    *s.mutableType() = PKT_STRING;
    TESTA_ASSERT(pp::prettyPrint(s) == "{\"pkey\":PKT_STRING}")
        (s).issue();
    *s.mutableOption() = PKO_AUTO_INCREMENT;
    TESTA_ASSERT(pp::prettyPrint(s) == "{\"pkey\":PKT_STRING+PKO_AUTO_INCREMENT}")
        (s).issue();
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKeyColumnSchema_pp);

void PrimaryKeyColumnSchema_validate(const string&)
{
    {
        PrimaryKeyColumnSchema s;
        Optional<Error> err = s.validate();
        TESTA_ASSERT(err.present())
            (s).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (*err).issue();
        TESTA_ASSERT(err->message() == "\"name\" is required.")
            (*err).issue();
    }
    {
        PrimaryKeyColumnSchema s("pkey", PKT_STRING, PKO_AUTO_INCREMENT);
        Optional<Error> err = s.validate();
        TESTA_ASSERT(err.present())
            (s).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (*err).issue();
        TESTA_ASSERT(err->message() == "PKO_AUTO_INCREMENT can only be applied on PKT_INTEGER, for primary key \"pkey\".")
            (*err).issue();
    }

}
TESTA_DEF_JUNIT_LIKE1(PrimaryKeyColumnSchema_validate);

void Schema_pp(const string&)
{
    Schema s;
    TESTA_ASSERT(pp::prettyPrint(s) == "[]")
        (s).issue();
    *s.append().mutableName() = "pkey";
    TESTA_ASSERT(pp::prettyPrint(s) == "[{\"pkey\":PKT_INTEGER}]")
        (s).issue();
}
TESTA_DEF_JUNIT_LIKE1(Schema_pp);

void Schema_validate(const string&)
{
    Schema s;
    {
        Optional<Error> err = s.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (s)(*err).issue();
        TESTA_ASSERT(err->message() == "Table schema must be nonempty.")
            (s)(*err).issue();
    }
    *s.append().mutableName() = "pkey";
    *s.back().mutableType() = PKT_STRING;
    *s.back().mutableOption() = PKO_AUTO_INCREMENT;
    {
        Optional<Error> err = s.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (s)(*err).issue();
        TESTA_ASSERT(err->message() == "PKO_AUTO_INCREMENT can only be applied on PKT_INTEGER, for primary key \"pkey\".")
            (s)(*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(Schema_validate);

void PrimaryKeyValue_none(const string&)
{
    PrimaryKeyValue v;
    TESTA_ASSERT(v.category() == PrimaryKeyValue::NONE)
        (v.category()).issue();
    TESTA_ASSERT(pp::prettyPrint(v) == "none")
        (v).issue();
    {
        Optional<Error> err = v.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (v)(*err).issue();
        TESTA_ASSERT(err->message() == "value is required.")
            (v)(*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKeyValue_none);

void PrimaryKeyValue_int(const string&)
{
    PrimaryKeyValue v = PrimaryKeyValue::toInteger(1);
    TESTA_ASSERT(v.category() == PrimaryKeyValue::INTEGER)
        (v.category()).issue();
    TESTA_ASSERT(pp::prettyPrint(v) == "1")
        (v).issue();
    *v.mutableInteger() = 2;
    TESTA_ASSERT(pp::prettyPrint(v) == "2")
        (v).issue();
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKeyValue_int);

void PrimaryKeyValue_str(const string&)
{
    PrimaryKeyValue v = PrimaryKeyValue::toStr(MemPiece::from("a"));
    TESTA_ASSERT(v.category() == PrimaryKeyValue::STRING)
        (v.category()).issue();
    TESTA_ASSERT(pp::prettyPrint(v) == "\"a\"")
        (v).issue();
    *v.mutableStr() = MemPiece::from("b");
    TESTA_ASSERT(pp::prettyPrint(v) == "\"b\"")
        (v).issue();
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKeyValue_str);

void PrimaryKeyValue_blob(const string&)
{
    PrimaryKeyValue v = PrimaryKeyValue::toBlob(MemPiece::from("a"));
    TESTA_ASSERT(v.category() == PrimaryKeyValue::BINARY)
        (v.category()).issue();
    TESTA_ASSERT(pp::prettyPrint(v) == "b\"a\"")
        (v).issue();
    *v.mutableBlob() = MemPiece::from("b");
    TESTA_ASSERT(pp::prettyPrint(v) == "b\"b\"")
        (v).issue();
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKeyValue_blob);

void PrimaryKeyValue_infmax(const string&)
{
    PrimaryKeyValue v = PrimaryKeyValue::toInfMax();
    TESTA_ASSERT(v.category() == PrimaryKeyValue::INF_MAX)
        (v.category()).issue();
    TESTA_ASSERT(v.isInfMax())
        (v).issue();
    TESTA_ASSERT(pp::prettyPrint(v) == "+inf")
        (v).issue();
    
    PrimaryKeyValue u;
    u.setInfMax();
    TESTA_ASSERT(u.isInfMax()).issue();
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKeyValue_infmax);

void PrimaryKeyValue_infmin(const string&)
{
    PrimaryKeyValue v = PrimaryKeyValue::toInfMin();
    TESTA_ASSERT(v.category() == PrimaryKeyValue::INF_MIN)
        (v.category()).issue();
    TESTA_ASSERT(v.isInfMin())
        (v).issue();
    TESTA_ASSERT(pp::prettyPrint(v) == "-inf")
        (v).issue();
    
    PrimaryKeyValue u;
    u.setInfMin();
    TESTA_ASSERT(u.isInfMin()).issue();
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKeyValue_infmin);

void PrimaryKeyValue_autoincr(const string&)
{
    PrimaryKeyValue v = PrimaryKeyValue::toAutoIncrement();
    TESTA_ASSERT(v.category() == PrimaryKeyValue::AUTO_INCR)
        (v.category()).issue();
    TESTA_ASSERT(v.isAutoIncrement())
        (v).issue();
    TESTA_ASSERT(pp::prettyPrint(v) == "auto-incr")
        (v).issue();
    
    PrimaryKeyValue u;
    u.setAutoIncrement();
    TESTA_ASSERT(u.isAutoIncrement()).issue();
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKeyValue_autoincr);

void PrimaryKeyColumn_pp(const string&)
{
    PrimaryKeyColumn c("pkey", PrimaryKeyValue::toInteger(1));
    TESTA_ASSERT(pp::prettyPrint(c) == "\"pkey\":1")
        (c).issue();
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKeyColumn_pp);

void PrimaryKeyColumn_validate(const string&)
{
    {
        PrimaryKeyColumn c;
        *c.mutableValue() = PrimaryKeyValue::toInteger(1);
        Optional<Error> err = c.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (c)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (c)(*err).issue();
        TESTA_ASSERT(err->message() == "name of primary-key column is required.")
            (c)(*err).issue();
    }
    {
        PrimaryKeyColumn c;
        *c.mutableName() = "pkey";
        Optional<Error> err = c.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (c)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (c)(*err).issue();
        TESTA_ASSERT(err->message() == "For primary-key column \"pkey\", value is required.")
            (c)(*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKeyColumn_validate);

void PrimaryKey_pp(const string&)
{
    PrimaryKey pk;
    *pk.append().mutableName() = "pkey";
    *pk.back().mutableValue()->mutableInteger() = 1;
    TESTA_ASSERT(pp::prettyPrint(pk) == "{\"pkey\":1}")
        (pk).issue();
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKey_pp);

void PrimaryKey_validate(const string&)
{
    PrimaryKey pk;
    {
        Optional<Error> err = pk.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (pk)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (pk)(*err).issue();
        TESTA_ASSERT(err->message() == "Primary key is required.")
            (pk)(*err).issue();
    }
    *pk.append().mutableName() = "pkey";
    {
        Optional<Error> err = pk.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (pk)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (pk)(*err).issue();
        TESTA_ASSERT(err->message() == "For primary-key column \"pkey\", value is required.")
            (pk)(*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKey_validate);

void TableMeta_pp(const string&)
{
    TableMeta meta("table");
    *meta.mutableSchema()->append().mutableName() = "pkey";
    *meta.mutableSchema()->back().mutableType() = PKT_INTEGER;
    TESTA_ASSERT(pp::prettyPrint(meta) == "{"
        "\"TableName\":\"table\",\"Schema\":[{\"pkey\":PKT_INTEGER}]}")
        (meta).issue();
}
TESTA_DEF_JUNIT_LIKE1(TableMeta_pp);

void TableMeta_validate(const string&)
{
    {
        TableMeta meta;
        *meta.mutableSchema()->append().mutableName() = "pkey";
        *meta.mutableSchema()->back().mutableType() = PKT_INTEGER;
        Optional<Error> err = meta.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (meta)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (meta)(*err).issue();
        TESTA_ASSERT(err->message() == "Table name is required.")
            (meta)(*err).issue();
    }
    {
        TableMeta meta;
        *meta.mutableTableName() = "table";
        Optional<Error> err = meta.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (meta)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (meta)(*err).issue();
        TESTA_ASSERT(err->message() == "Table schema must be nonempty.")
            (meta)(*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(TableMeta_validate);

void TableOptions_TTL(const string&)
{
    TableOptions opt;
    *opt.mutableTimeToLive() = Duration::fromSec(1);
    TESTA_ASSERT(pp::prettyPrint(opt) == "{\"TimeToLive\":1}")
        (opt).issue();

    opt.mutableTimeToLive()->reset();
    TESTA_ASSERT(!opt.timeToLive().present())
        (opt).issue();
}
TESTA_DEF_JUNIT_LIKE1(TableOptions_TTL);

void TableOptions_maxversions(const string&)
{
    TableOptions opt;
    *opt.mutableMaxVersions() = 1;
    TESTA_ASSERT(pp::prettyPrint(opt) == "{\"MaxVersions\":1}")
        (opt).issue();

    opt.mutableMaxVersions()->reset();
    TESTA_ASSERT(!opt.maxVersions().present())
        (opt).issue();
}
TESTA_DEF_JUNIT_LIKE1(TableOptions_maxversions);

void TableOptions_BloomFilterType(const string&)
{
    TableOptions opt;
    *opt.mutableBloomFilterType() = BFT_CELL;
    TESTA_ASSERT(pp::prettyPrint(opt) == "{\"BloomFilterType\":BFT_CELL}")
        (opt).issue();

    opt.mutableBloomFilterType()->reset();
    TESTA_ASSERT(!opt.bloomFilterType().present())
        (opt).issue();
}
TESTA_DEF_JUNIT_LIKE1(TableOptions_BloomFilterType);

void TableOptions_BlockSize(const string&)
{
    TableOptions opt;
    *opt.mutableBlockSize() = 1;
    TESTA_ASSERT(pp::prettyPrint(opt) == "{\"BlockSize\":1}")
        (opt).issue();

    opt.mutableBlockSize()->reset();
    TESTA_ASSERT(!opt.blockSize().present())
        (opt).issue();
}
TESTA_DEF_JUNIT_LIKE1(TableOptions_BlockSize);

void TableOptions_MaxTimeDeviation(const string&)
{
    TableOptions opt;
    *opt.mutableMaxTimeDeviation() = Duration::fromSec(1);
    TESTA_ASSERT(pp::prettyPrint(opt) == "{\"MaxTimeDeviation\":1}")
        (opt).issue();

    opt.mutableMaxTimeDeviation()->reset();
    TESTA_ASSERT(!opt.maxTimeDeviation().present())
        (opt).issue();
}
TESTA_DEF_JUNIT_LIKE1(TableOptions_MaxTimeDeviation);

void TableOptions_validate(const string&)
{
    {
        TableOptions opt;
        *opt.mutableTimeToLive() = Duration::fromUsec(1);
        Optional<Error> err = opt.validate();
        TESTA_ASSERT(err.present())
            (opt).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (opt)(*err).issue();
    }
    {
        TableOptions opt;
        *opt.mutableTimeToLive() = Duration::fromUsec(0);
        Optional<Error> err = opt.validate();
        TESTA_ASSERT(err.present())
            (opt).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (opt)(*err).issue();
    }
    {
        TableOptions opt;
        *opt.mutableMaxVersions() = 0;
        Optional<Error> err = opt.validate();
        TESTA_ASSERT(err.present())
            (opt).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (opt)(*err).issue();
    }
    {
        TableOptions opt;
        *opt.mutableBlockSize() = 0;
        Optional<Error> err = opt.validate();
        TESTA_ASSERT(err.present())
            (opt).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (opt)(*err).issue();
    }
    {
        TableOptions opt;
        *opt.mutableMaxTimeDeviation() = Duration::fromUsec(1);
        Optional<Error> err = opt.validate();
        TESTA_ASSERT(err.present())
            (opt).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (opt)(*err).issue();
    }
    {
        TableOptions opt;
        *opt.mutableMaxTimeDeviation() = Duration::fromSec(0);
        Optional<Error> err = opt.validate();
        TESTA_ASSERT(err.present())
            (opt).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (opt)(*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(TableOptions_validate);

void CapacityUnit_validate(const string&)
{
    {
        CapacityUnit cu;
        *cu.mutableRead() = -1;
        Optional<Error> err = cu.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (*err).issue();
        TESTA_ASSERT(err->message() == "read capacity unit must be positive.")
            (cu)(*err).issue();
    }
    {
        CapacityUnit cu;
        *cu.mutableWrite() = -1;
        Optional<Error> err = cu.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (*err).issue();
        TESTA_ASSERT(err->message() == "write capacity unit must be positive.")
            (cu)(*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(CapacityUnit_validate);

void AttributeValue_none(const string&)
{
    AttributeValue v;
    TESTA_ASSERT(v.category() == AttributeValue::NONE)
        (v.category()).issue();
    TESTA_ASSERT(pp::prettyPrint(v) == "none")
        (v).issue();
    {
        Optional<Error> err = v.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (v)(*err).issue();
        TESTA_ASSERT(err->message() == "value is required.")
            (v)(*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(AttributeValue_none);

void AttributeValue_int(const string&)
{
    AttributeValue v = AttributeValue::toInteger(1);
    TESTA_ASSERT(v.category() == AttributeValue::INTEGER)
        (v.category()).issue();
    TESTA_ASSERT(pp::prettyPrint(v) == "1")
        (v).issue();
    *v.mutableInteger() = 2;
    TESTA_ASSERT(pp::prettyPrint(v) == "2")
        (v).issue();
}
TESTA_DEF_JUNIT_LIKE1(AttributeValue_int);

void AttributeValue_str(const string&)
{
    AttributeValue v = AttributeValue::toStr(MemPiece::from("a"));
    TESTA_ASSERT(v.category() == AttributeValue::STRING)
        (v.category()).issue();
    TESTA_ASSERT(pp::prettyPrint(v) == "\"a\"")
        (v).issue();
    *v.mutableStr() = MemPiece::from("b");
    TESTA_ASSERT(pp::prettyPrint(v) == "\"b\"")
        (v).issue();
}
TESTA_DEF_JUNIT_LIKE1(AttributeValue_str);

void AttributeValue_blob(const string&)
{
    AttributeValue v = AttributeValue::toBlob(MemPiece::from("a"));
    TESTA_ASSERT(v.category() == AttributeValue::BINARY)
        (v.category()).issue();
    TESTA_ASSERT(pp::prettyPrint(v) == "b\"a\"")
        (v).issue();
    *v.mutableBlob() = MemPiece::from("b");
    TESTA_ASSERT(pp::prettyPrint(v) == "b\"b\"")
        (v).issue();
}
TESTA_DEF_JUNIT_LIKE1(AttributeValue_blob);

void AttributeValue_bool(const string&)
{
    AttributeValue v = AttributeValue::toBoolean(true);
    TESTA_ASSERT(v.category() == AttributeValue::BOOLEAN)
        (v.category()).issue();
    TESTA_ASSERT(pp::prettyPrint(v) == "true")
        (v).issue();
    *v.mutableBoolean() = false;
    TESTA_ASSERT(pp::prettyPrint(v) == "false")
        (v).issue();
}
TESTA_DEF_JUNIT_LIKE1(AttributeValue_bool);

void AttributeValue_floating(const string&)
{
    AttributeValue v = AttributeValue::toFloatPoint(1.2);
    TESTA_ASSERT(v.category() == AttributeValue::FLOATING_POINT)
        (v.category()).issue();
    TESTA_ASSERT(pp::prettyPrint(v) == "1.2000")
        (v).issue();
    *v.mutableFloatPoint() = 2.1;
    TESTA_ASSERT(pp::prettyPrint(v) == "2.1000")
        (v).issue();
}
TESTA_DEF_JUNIT_LIKE1(AttributeValue_floating);

void Attribute_pp(const string&)
{
    Attribute v("attr", AttributeValue::toInteger(1), UtcTime::fromMsec(2));
    TESTA_ASSERT(pp::prettyPrint(v) == "{"
        "\"Name\":\"attr\","
        "\"Value\":1,"
        "\"Timestamp\":\"1970-01-01T00:00:00.002000Z\"}")
        (v).issue();
}
TESTA_DEF_JUNIT_LIKE1(Attribute_pp);

void Attribute_validate(const string&)
{
    {
        Attribute v("", AttributeValue::toInteger(1));
        Optional<Error> err = v.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (v)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (v)(*err).issue();
        TESTA_ASSERT(err->message() == "Attribute name must be nonempty.")
            (v)(*err).issue();
    }
    {
        Attribute v("attr", AttributeValue());
        Optional<Error> err = v.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (v)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (v)(*err).issue();
        TESTA_ASSERT(err->message() == "For column \"attr\", value is required.")
            (v)(*err).issue();
    }
    {
        Attribute v("attr", AttributeValue::toInteger(1), UtcTime::fromMsec(-1));
        Optional<Error> err = v.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (v)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (v)(*err).issue();
        TESTA_ASSERT(err->message() == "Timestamp of column \"attr\" must be positive.")
            (v)(*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(Attribute_validate);

void Row_pp(const string&)
{
    Row row;
    *row.mutablePrimaryKey()->append().mutableName() = "pkey";
    *row.mutablePrimaryKey()->back().mutableValue() = PrimaryKeyValue::toInteger(1);
    *row.mutableAttributes()->append().mutableName() = "attr";
    *row.mutableAttributes()->back().mutableValue() = AttributeValue::toInteger(2);
    TESTA_ASSERT(pp::prettyPrint(row) == "{\"PrimaryKey\":{\"pkey\":1},\"Attributes\":[{\"Name\":\"attr\",\"Value\":2}]}")
        (row).issue();
}
TESTA_DEF_JUNIT_LIKE1(Row_pp);

void Row_validate(const string&)
{
    Row row;
    {
        Optional<Error> err = row.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (row)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (row)(*err).issue();
        TESTA_ASSERT(err->message() == "Primary key is required.")
            (row)(*err).issue();
    }
    row.mutablePrimaryKey()->append() =
        PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(1));
    *row.mutableAttributes()->append().mutableName() = "attr";
    {
        Optional<Error> err = row.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (row)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (row)(*err).issue();
        TESTA_ASSERT(err->message() == "For column \"attr\", value is required.")
            (row)(*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(Row_validate);

void TimeRange_pp(const string&)
{
    TimeRange tr(UtcTime::fromMsec(1), UtcTime::fromMsec(2));
    TESTA_ASSERT(pp::prettyPrint(tr) == "[\"1970-01-01T00:00:00.001000Z\",\"1970-01-01T00:00:00.002000Z\"]")
        (tr).issue();
}
TESTA_DEF_JUNIT_LIKE1(TimeRange_pp);

void TimeRange_validate(const string&)
{
    {
        TimeRange tr(UtcTime::fromUsec(1), UtcTime::fromMsec(1));
        Optional<Error> err = tr.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (tr)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (tr)(*err).issue();
        TESTA_ASSERT(err->message() == "Start of time ranges must be integral multiple of milliseconds.")
            (tr)(*err).issue();
    }
    {
        TimeRange tr(UtcTime::fromMsec(1), UtcTime::fromUsec(1001));
        Optional<Error> err = tr.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (tr)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (tr)(*err).issue();
        TESTA_ASSERT(err->message() == "End of time ranges must be integral multiple of milliseconds.")
            (tr)(*err).issue();
    }
    {
        TimeRange tr(UtcTime::fromMsec(2), UtcTime::fromMsec(1));
        Optional<Error> err = tr.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (tr)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (tr)(*err).issue();
        TESTA_ASSERT(err->message() == "Start of time ranges must be in advance of their ends.")
            (tr)(*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(TimeRange_validate);

void Split_pp(const string&)
{
    Split split;
    split.mutableLowerBound()->reset(new PrimaryKey());
    (*split.mutableLowerBound())->append() = PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(1));
    split.mutableUpperBound()->reset(new PrimaryKey());
    (*split.mutableUpperBound())->append() = PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(2));
    *split.mutableLocation() = "youguess";
    TESTA_ASSERT(pp::prettyPrint(split) == "{\"Location\":\"youguess\",\"LowerBound\":{\"pkey\":1},\"UpperBound\":{\"pkey\":2}}")
        (split).issue();
}
TESTA_DEF_JUNIT_LIKE1(Split_pp);

void Split_validate(const string&)
{
    {
        Split s;
        s.mutableLowerBound()->reset(new PrimaryKey());
        (*s.mutableLowerBound())->append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(1));
        s.mutableUpperBound()->reset(new PrimaryKey());
        (*s.mutableUpperBound())->append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(2));
        (*s.mutableUpperBound())->append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(3));
        Optional<Error> err = s.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (s)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (s)(*err).issue();
        TESTA_ASSERT(err->message() == "Lower bound of a split must be of the same length of the upper bound of that split.")
            (s)(*err).issue();
    }
    {
        Split s;
        s.mutableLowerBound()->reset(new PrimaryKey());
        (*s.mutableLowerBound())->append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(1));
        s.mutableUpperBound()->reset(new PrimaryKey());
        (*s.mutableUpperBound())->append() = PrimaryKeyColumn("pk1", PrimaryKeyValue::toInteger(2));
        Optional<Error> err = s.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (s)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (s)(*err).issue();
        TESTA_ASSERT(err->message() == "Lower bound of a split must have the same names of the upper bound of that split.")
            (s)(*err).issue();
    }
    {
        Split s;
        s.mutableLowerBound()->reset(new PrimaryKey());
        (*s.mutableLowerBound())->append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(1));
        s.mutableUpperBound()->reset(new PrimaryKey());
        (*s.mutableUpperBound())->append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toStr(MemPiece::from("a")));
        Optional<Error> err = s.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (s)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (s)(*err).issue();
        TESTA_ASSERT(err->message() == "Lower bound of a split must have the same types of the upper bound of that split.")
            (s)(*err).issue();
    }
    {
        Split s;
        s.mutableLowerBound()->reset(new PrimaryKey());
        (*s.mutableLowerBound())->append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(1));
        s.mutableUpperBound()->reset(new PrimaryKey());
        (*s.mutableUpperBound())->append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(0));
        Optional<Error> err = s.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (s)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (s)(*err).issue();
        TESTA_ASSERT(err->message() == "Lower bound of a split must be smaller than the upper bound of that split.")
            (s)(*err).issue();
    }
    {
        Split s;
        s.mutableLowerBound()->reset(new PrimaryKey());
        (*s.mutableLowerBound())->append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toAutoIncrement());
        s.mutableUpperBound()->reset(new PrimaryKey());
        (*s.mutableUpperBound())->append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toAutoIncrement());
        Optional<Error> err = s.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (s)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (s)(*err).issue();
        TESTA_ASSERT(err->message() == "Lower bound of a split must be comparable with the upper bound.")
            (s)(*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(Split_validate);

void CreateTableRequest_pp(const string&)
{
    CreateTableRequest req;
    *req.mutableMeta()->mutableTableName() = "table";
    *req.mutableMeta()->mutableSchema()->append().mutableName() = "pkey";
    *req.mutableMeta()->mutableSchema()->back().mutableType() = PKT_STRING;
    req.mutableShardSplitPoints()->append().append() = PrimaryKeyColumn("pkey", PrimaryKeyValue::toStr(MemPiece::from("a")));
    TESTA_ASSERT(pp::prettyPrint(req) == "{\"API\":\"CreateTableRequest\","
        "\"Meta\":{\"TableName\":\"table\",\"Schema\":[{\"pkey\":PKT_STRING}]},"
        "\"Options\":{\"ReservedThroughput\":{\"Read\":0,\"Write\":0},\"MaxVersions\":1},"
        "\"ShardSplitPoints\":[{\"pkey\":\"a\"}]}")
        (req).issue();
}
TESTA_DEF_JUNIT_LIKE1(CreateTableRequest_pp);

void CreateTableRequest_validate(const string&)
{
    {
        CreateTableRequest req;
        *req.mutableMeta()->mutableTableName() = "table";
        *req.mutableMeta()->mutableSchema()->append().mutableName() = "pk0";
        *req.mutableMeta()->mutableSchema()->back().mutableType() = PKT_STRING;
        req.mutableShardSplitPoints()->append().append() = PrimaryKeyColumn("pk1", PrimaryKeyValue::toStr(MemPiece::from("a")));
        Optional<Error> err = req.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (req)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (req)(*err).issue();
        TESTA_ASSERT(err->message() == "Shard split points contains \"pk1\", which is different with that in the schema.")
            (req)(*err).issue();
    }
    {
        CreateTableRequest req;
        *req.mutableMeta()->mutableTableName() = "table";
        *req.mutableMeta()->mutableSchema()->append().mutableName() = "pk0";
        *req.mutableMeta()->mutableSchema()->back().mutableType() = PKT_STRING;
        *req.mutableOptions()->mutableMaxVersions() = 1;
        req.mutableShardSplitPoints()->append().append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(1));
        Optional<Error> err = req.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (req)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (req)(*err).issue();
        TESTA_ASSERT(err->message() == "Type of primary-key column \"pk0\" mismatches that in schema.")
            (req)(*err).issue();
    }
    {
        CreateTableRequest req;
        *req.mutableMeta()->mutableTableName() = "table";
        *req.mutableMeta()->mutableSchema()->append().mutableName() = "pk0";
        *req.mutableMeta()->mutableSchema()->back().mutableType() = PKT_INTEGER;
        *req.mutableOptions()->mutableMaxVersions() = 1;
        req.mutableShardSplitPoints()->append().append() =
            PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(1));
        req.mutableShardSplitPoints()->back().append() =
            PrimaryKeyColumn("pk1", PrimaryKeyValue::toInteger(2));
        Optional<Error> err = req.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (req)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (req)(*err).issue();
        TESTA_ASSERT(err->message() == "Length of shard split points must be exactly one.")
            (req)(*err).issue();
    }
    {
        CreateTableRequest req;
        *req.mutableMeta()->mutableTableName() = "table";
        *req.mutableMeta()->mutableSchema()->append().mutableName() = "pk0";
        *req.mutableMeta()->mutableSchema()->back().mutableType() = PKT_INTEGER;
        *req.mutableOptions()->mutableMaxVersions() = 1;
        req.mutableShardSplitPoints()->append().append() =
            PrimaryKeyColumn("pk0", PrimaryKeyValue::toInfMin());
        Optional<Error> err = req.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (req)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (req)(*err).issue();
        TESTA_ASSERT(err->message() == "Shard split points contains an unreal value type -INF.")
            (req)(*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(CreateTableRequest_validate);

void Condition_RowExistenceExpectation_pp(const string&)
{
    TESTA_ASSERT(pp::prettyPrint(Condition::IGNORE) == "IGNORE")
        (Condition::IGNORE).issue();
    TESTA_ASSERT(pp::prettyPrint(Condition::EXPECT_EXIST) == "EXPECT_EXIST")
        (Condition::EXPECT_EXIST).issue();
    TESTA_ASSERT(pp::prettyPrint(Condition::EXPECT_NOT_EXIST) == "EXPECT_NOT_EXIST")
        (Condition::EXPECT_NOT_EXIST).issue();
}
TESTA_DEF_JUNIT_LIKE1(Condition_RowExistenceExpectation_pp);

void Condition_pp(const string&)
{
    {
        Condition c;
        TESTA_ASSERT(pp::prettyPrint(c) == "{\"RowCondition\":IGNORE}")
            (c).issue();
    }
    {
        Condition c;
        *c.mutableRowCondition() = Condition::EXPECT_EXIST;
        TESTA_ASSERT(pp::prettyPrint(c) == "{\"RowCondition\":EXPECT_EXIST}")
            (c).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(Condition_pp);

void SingleColumnCondition_pp(const string&)
{
    SingleColumnCondition c;
    *c.mutableColumnName() = "col";
    *c.mutableAttributeValue() = AttributeValue::toInteger(123);
    TESTA_ASSERT(pp::prettyPrint(c) == "{"
        "\"Relation\":EQUAL,"
        "\"ColumnName\":\"col\","
        "\"ColumnValue\":123,"
        "\"PassIfMissing\":false,"
        "\"LatestVersionOnly\":true}")
        (c).issue();
}
TESTA_DEF_JUNIT_LIKE1(SingleColumnCondition_pp);

void CompositeColumnCondition_pp(const string&)
{
    CompositeColumnCondition c;
    c.mutableChildren()->append().reset(
        new SingleColumnCondition(
            "col",
            SingleColumnCondition::EQUAL,
            AttributeValue::toInteger(123)));
    TESTA_ASSERT(pp::prettyPrint(c) == "{\"Operator\":AND,"
        "\"Children\":[{\"Relation\":EQUAL,"
        "\"ColumnName\":\"col\","
        "\"ColumnValue\":123,"
        "\"PassIfMissing\":false,"
        "\"LatestVersionOnly\":true}]}")
        (c).issue();
        
}
TESTA_DEF_JUNIT_LIKE1(CompositeColumnCondition_pp);

void PointQueryCriterion_pp(const string&)
{
    PointQueryCriterion cri;
    cri.mutablePrimaryKey()->append() = 
        PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(123));
    *cri.mutableTable() = "test-table";
    cri.mutableColumnsToGet()->append() = "attr";
    *cri.mutableMaxVersions() = 1;
    *cri.mutableTimeRange() = TimeRange(UtcTime::fromSec(1), UtcTime::fromSec(2));
    *cri.mutableCacheBlocks() = true;
    cri.mutableFilter()->reset(new SingleColumnCondition(
            "attr",
            SingleColumnCondition::EQUAL,
            AttributeValue::toInteger(456)));
    TESTA_ASSERT(pp::prettyPrint(cri) == "{"
        "\"TableName\":\"test-table\","
        "\"ColumnsToGet\":[\"attr\"],"
        "\"MaxVersions\":1,"
        "\"TimeRange\":[\"1970-01-01T00:00:01.000000Z\",\"1970-01-01T00:00:02.000000Z\"],"
        "\"CacheBlocks\":true,"
        "\"Filter\":{"
            "\"Relation\":EQUAL,"
            "\"ColumnName\":\"attr\","
            "\"ColumnValue\":456,"
            "\"PassIfMissing\":false,"
            "\"LatestVersionOnly\":true},"
        "\"PrimaryKey\":{\"pkey\":123}}")
        (cri).issue();
}
TESTA_DEF_JUNIT_LIKE1(PointQueryCriterion_pp);

void QueryCriterion_validate(const string&)
{
    PointQueryCriterion cri;
    cri.mutablePrimaryKey()->append() =
        PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(123));
    {
        PointQueryCriterion c = cri;
        *c.mutableMaxVersions() = 1;
        Optional<Error> err = c.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (*err)(c).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (*err)(c).issue();
        TESTA_ASSERT(err->message() == "Table name is required.")
            (*err)(c).issue();
    }
    {
        PointQueryCriterion c = cri;
        *c.mutableTable() = "test-table";
        Optional<Error> err = c.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (*err)(c).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (*err)(c).issue();
        TESTA_ASSERT(err->message() == "Either MaxVersions or TimeRange is required.")
            (*err)(c).issue();
    }
    {
        PointQueryCriterion c = cri;
        *c.mutableTable() = "test-table";
        *c.mutableMaxVersions() = 0;
        Optional<Error> err = c.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (*err)(c).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (*err)(c).issue();
        TESTA_ASSERT(err->message() == "MaxVersions must be positive.")
            (*err)(c).issue();
    }
    {
        PointQueryCriterion c = cri;
        *c.mutableTable() = "test-table";
        *c.mutableMaxVersions() = 1;
        c.mutableColumnsToGet()->append() = "";
        Optional<Error> err = c.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (*err)(c).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (*err)(c).issue();
        TESTA_ASSERT(err->message() == "Columns in ColumnsToGet must be nonempty.")
            (*err)(c).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(QueryCriterion_validate);

void PointQueryCriterion_validate(const string&)
{
    PointQueryCriterion cri;
    *cri.mutableTable() = "test-table";
    *cri.mutableMaxVersions() = 1;
    Optional<Error> err = cri.validate();
    TESTA_ASSERT(err.present()).issue();
    TESTA_ASSERT(err->httpStatus() < 0)
        (*err)(cri).issue();
    TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
        (*err)(cri).issue();
    TESTA_ASSERT(err->message() == "Primary key is required.")
        (*err)(cri).issue();
}
TESTA_DEF_JUNIT_LIKE1(PointQueryCriterion_validate);

void RangeQueryCriterion_pp(const string&)
{
    RangeQueryCriterion cri;
    cri.mutableInclusiveStart()->append() =
        PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(1));
    cri.mutableExclusiveEnd()->append() =
        PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(2));
    *cri.mutableLimit() = 3;
    TESTA_ASSERT(pp::prettyPrint(cri) == "{"
        "\"Direction\":FORWARD,"
        "\"Start\":{\"pkey\":1},"
        "\"End\":{\"pkey\":2},"
        "\"Limit\":3}")
        (cri).issue();
}
TESTA_DEF_JUNIT_LIKE1(RangeQueryCriterion_pp);

void RangeQueryCriterion_validate(const string&)
{
    {
        RangeQueryCriterion cri;
        *cri.mutableTable() = "test-table";
        *cri.mutableMaxVersions() = 1;
        cri.mutableInclusiveStart()->append() =
            PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(1));
        cri.mutableExclusiveEnd()->append() =
            PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(2));
        cri.mutableExclusiveEnd()->append() =
            PrimaryKeyColumn("pk1", PrimaryKeyValue::toInteger(2));
        Optional<Error> err = cri.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (*err)(cri).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (*err)(cri).issue();
        TESTA_ASSERT(err->message() == "Start primary key must be of the same length of that of the end.")
            (*err)(cri).issue();
    }
    {
        RangeQueryCriterion cri;
        *cri.mutableTable() = "test-table";
        *cri.mutableMaxVersions() = 1;
        cri.mutableInclusiveStart()->append() =
            PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(1));
        cri.mutableExclusiveEnd()->append() =
            PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(0));
        Optional<Error> err = cri.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (*err)(cri).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (*err)(cri).issue();
        TESTA_ASSERT(err->message() == "Start primary key should be less than or equals to the end in a forward range.")
            (*err)(cri).issue();
    }
    {
        RangeQueryCriterion cri;
        *cri.mutableTable() = "test-table";
        *cri.mutableMaxVersions() = 1;
        *cri.mutableDirection() = RangeQueryCriterion::BACKWARD;
        cri.mutableInclusiveStart()->append() =
            PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(0));
        cri.mutableExclusiveEnd()->append() =
            PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(1));
        Optional<Error> err = cri.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (*err)(cri).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (*err)(cri).issue();
        TESTA_ASSERT(err->message() == "Start primary key should be greater than or equals to the end in a backward range.")
            (*err)(cri).issue();
    }
    {
        RangeQueryCriterion cri;
        *cri.mutableTable() = "test-table";
        *cri.mutableMaxVersions() = 1;
        cri.mutableInclusiveStart()->append() =
            PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(0));
        cri.mutableExclusiveEnd()->append() =
            PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(1));
        *cri.mutableLimit() = 0;
        Optional<Error> err = cri.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (*err)(cri).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (*err)(cri).issue();
        TESTA_ASSERT(err->message() == "Limit of GetRange must be positive.")
            (*err)(cri).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(RangeQueryCriterion_validate);

} // namespace core
} // namespace tablestore
} // namespace aliyun

