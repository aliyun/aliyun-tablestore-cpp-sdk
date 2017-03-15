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
#include "core/types.hpp"
#include "util/prettyprint.hpp"
#include "util/foreach.hpp"
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

void Action_pp()
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

void PrimaryKeyType_pp()
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

void PrimaryKeyOption_pp()
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

void BloomFilterType_pp()
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

void DequeBasedVector_move()
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

void PrimaryKeyColumnSchema_pp()
{
    PrimaryKeyColumnSchema s;
    s.mutableName() = "pkey";
    s.mutableType() = PKT_STRING;
    TESTA_ASSERT(pp::prettyPrint(s) == "{\"pkey\":PKT_STRING}")
        (s).issue();
    s.mutableOption() = PKO_AUTO_INCREMENT;
    TESTA_ASSERT(pp::prettyPrint(s) == "{\"pkey\":PKT_STRING+PKO_AUTO_INCREMENT}")
        (s).issue();
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKeyColumnSchema_pp);

void PrimaryKeyColumnSchema_validate()
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

void PrimaryKeySchema_pp()
{
    PrimaryKeySchema s;
    TESTA_ASSERT(pp::prettyPrint(s) == "[]")
        (s).issue();
    s.append().mutableName() = "pkey";
    TESTA_ASSERT(pp::prettyPrint(s) == "[{\"pkey\":PKT_INTEGER}]")
        (s).issue();
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKeySchema_pp);

void PrimaryKeySchema_validate()
{
    PrimaryKeySchema s;
    {
        Optional<Error> err = s.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (s)(*err).issue();
        TESTA_ASSERT(err->message() == "Table schema must be nonempty.")
            (s)(*err).issue();
    }
    s.append().mutableName() = "pkey";
    s.back().mutableType() = PKT_STRING;
    s.back().mutableOption() = PKO_AUTO_INCREMENT;
    {
        Optional<Error> err = s.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (s)(*err).issue();
        TESTA_ASSERT(err->message() == "PKO_AUTO_INCREMENT can only be applied on PKT_INTEGER, for primary key \"pkey\".")
            (s)(*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKeySchema_validate);

void PrimaryKeyValue_none()
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
        TESTA_ASSERT(err->message() == "value is not set.")
            (v)(*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKeyValue_none);

void PrimaryKeyValue_int()
{
    PrimaryKeyValue v = PrimaryKeyValue::toInteger(1);
    TESTA_ASSERT(v.category() == PrimaryKeyValue::INTEGER)
        (v.category()).issue();
    TESTA_ASSERT(pp::prettyPrint(v) == "1")
        (v).issue();
    v.mutableInteger() = 2;
    TESTA_ASSERT(pp::prettyPrint(v) == "2")
        (v).issue();
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKeyValue_int);

void PrimaryKeyValue_str()
{
    PrimaryKeyValue v = PrimaryKeyValue::toStr(MemPiece::from("a"));
    TESTA_ASSERT(v.category() == PrimaryKeyValue::STRING)
        (v.category()).issue();
    TESTA_ASSERT(pp::prettyPrint(v) == "\"a\"")
        (v).issue();
    v.mutableStr() = MemPiece::from("b");
    TESTA_ASSERT(pp::prettyPrint(v) == "\"b\"")
        (v).issue();
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKeyValue_str);

void PrimaryKeyValue_blob()
{
    PrimaryKeyValue v = PrimaryKeyValue::toBlob(MemPiece::from("a"));
    TESTA_ASSERT(v.category() == PrimaryKeyValue::BINARY)
        (v.category()).issue();
    TESTA_ASSERT(pp::prettyPrint(v) == "b\"a\"")
        (v).issue();
    v.mutableBlob() = MemPiece::from("b");
    TESTA_ASSERT(pp::prettyPrint(v) == "b\"b\"")
        (v).issue();
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKeyValue_blob);

void PrimaryKeyValue_infmax()
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

void PrimaryKeyValue_infmin()
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

void PrimaryKeyValue_autoincr()
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

void PrimaryKeyColumn_pp()
{
    PrimaryKeyColumn c("pkey", PrimaryKeyValue::toInteger(1));
    TESTA_ASSERT(pp::prettyPrint(c) == "\"pkey\":1")
        (c).issue();
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKeyColumn_pp);

void PrimaryKeyColumn_validate()
{
    {
        PrimaryKeyColumn c;
        c.mutableValue() = PrimaryKeyValue::toInteger(1);
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
        c.mutableName() = "pkey";
        Optional<Error> err = c.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (c)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (c)(*err).issue();
        TESTA_ASSERT(err->message() == "For primary-key column \"pkey\", value is not set.")
            (c)(*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKeyColumn_validate);

void PrimaryKey_pp()
{
    PrimaryKey pk;
    pk.append().mutableName() = "pkey";
    pk.back().mutableValue().mutableInteger() = 1;
    TESTA_ASSERT(pp::prettyPrint(pk) == "{\"pkey\":1}")
        (pk).issue();
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKey_pp);

void PrimaryKey_validate()
{
    PrimaryKey pk;
    {
        Optional<Error> err = pk.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (pk)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (pk)(*err).issue();
        TESTA_ASSERT(err->message() == "Primary key must be nonempty.")
            (pk)(*err).issue();
    }
    pk.append().mutableName() = "pkey";
    {
        Optional<Error> err = pk.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (pk)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (pk)(*err).issue();
        TESTA_ASSERT(err->message() == "For primary-key column \"pkey\", value is not set.")
            (pk)(*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(PrimaryKey_validate);

void TableMeta_pp()
{
    TableMeta meta("table");
    meta.mutableSchema().append().mutableName() = "pkey";
    meta.mutableSchema().back().mutableType() = PKT_INTEGER;
    TESTA_ASSERT(pp::prettyPrint(meta) == "{\"TableName\":\"table\",\"Schema\":[{\"pkey\":PKT_INTEGER}]}")
        (meta).issue();
}
TESTA_DEF_JUNIT_LIKE1(TableMeta_pp);

void TableMeta_validate()
{
    {
        TableMeta meta;
        meta.mutableSchema().append().mutableName() = "pkey";
        meta.mutableSchema().back().mutableType() = PKT_INTEGER;
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
        meta.mutableTableName() = "table";
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

void TableOptions_TTL()
{
    TableOptions opt;
    opt.mutableTimeToLive() = Duration::fromSec(1);
    TESTA_ASSERT(pp::prettyPrint(opt) == "{\"ReservedThroughput\":{\"Read\":0,\"Write\":0},\"TimeToLive\":1}")
        (opt).issue();

    opt.mutableTimeToLive().clear();
    TESTA_ASSERT(!opt.timeToLive().present())
        (opt).issue();
}
TESTA_DEF_JUNIT_LIKE1(TableOptions_TTL);

void TableOptions_maxversions()
{
    TableOptions opt;
    opt.mutableMaxVersions() = 1;
    TESTA_ASSERT(pp::prettyPrint(opt) == "{\"ReservedThroughput\":{\"Read\":0,\"Write\":0},\"MaxVersions\":1}")
        (opt).issue();

    opt.mutableMaxVersions().clear();
    TESTA_ASSERT(!opt.maxVersions().present())
        (opt).issue();
}
TESTA_DEF_JUNIT_LIKE1(TableOptions_maxversions);

void TableOptions_BloomFilterType()
{
    TableOptions opt;
    opt.mutableBloomFilterType() = BFT_CELL;
    TESTA_ASSERT(pp::prettyPrint(opt) == "{\"ReservedThroughput\":{\"Read\":0,\"Write\":0},\"BloomFilterType\":BFT_CELL}")
        (opt).issue();

    opt.mutableBloomFilterType().clear();
    TESTA_ASSERT(!opt.bloomFilterType().present())
        (opt).issue();
}
TESTA_DEF_JUNIT_LIKE1(TableOptions_BloomFilterType);

void TableOptions_BlockSize()
{
    TableOptions opt;
    opt.mutableBlockSize() = 1;
    TESTA_ASSERT(pp::prettyPrint(opt) == "{\"ReservedThroughput\":{\"Read\":0,\"Write\":0},\"BlockSize\":1}")
        (opt).issue();

    opt.mutableBlockSize().clear();
    TESTA_ASSERT(!opt.blockSize().present())
        (opt).issue();
}
TESTA_DEF_JUNIT_LIKE1(TableOptions_BlockSize);

void TableOptions_MaxTimeDeviation()
{
    TableOptions opt;
    opt.mutableMaxTimeDeviation() = Duration::fromSec(1);
    TESTA_ASSERT(pp::prettyPrint(opt) == "{\"ReservedThroughput\":{\"Read\":0,\"Write\":0},\"MaxTimeDeviation\":1}")
        (opt).issue();

    opt.mutableMaxTimeDeviation().clear();
    TESTA_ASSERT(!opt.maxTimeDeviation().present())
        (opt).issue();
}
TESTA_DEF_JUNIT_LIKE1(TableOptions_MaxTimeDeviation);

void TableOptions_validate()
{
    {
        TableOptions opt;
        opt.mutableTimeToLive() = Duration::fromUsec(1);
        Optional<Error> err = opt.validate();
        TESTA_ASSERT(err.present())
            (opt).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (opt)(*err).issue();
    }
    {
        TableOptions opt;
        opt.mutableTimeToLive() = Duration::fromUsec(0);
        Optional<Error> err = opt.validate();
        TESTA_ASSERT(err.present())
            (opt).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (opt)(*err).issue();
    }
    {
        TableOptions opt;
        opt.mutableMaxVersions() = 0;
        Optional<Error> err = opt.validate();
        TESTA_ASSERT(err.present())
            (opt).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (opt)(*err).issue();
    }
    {
        TableOptions opt;
        opt.mutableBlockSize() = 0;
        Optional<Error> err = opt.validate();
        TESTA_ASSERT(err.present())
            (opt).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (opt)(*err).issue();
    }
    {
        TableOptions opt;
        opt.mutableMaxTimeDeviation() = Duration::fromUsec(1);
        Optional<Error> err = opt.validate();
        TESTA_ASSERT(err.present())
            (opt).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (opt)(*err).issue();
    }
    {
        TableOptions opt;
        opt.mutableMaxTimeDeviation() = Duration::fromSec(0);
        Optional<Error> err = opt.validate();
        TESTA_ASSERT(err.present())
            (opt).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (opt)(*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(TableOptions_validate);

void CapacityUnit_validate()
{
    {
        CapacityUnit cu;
        cu.mutableRead() = -1;
        Optional<Error> err = cu.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (*err).issue();
        TESTA_ASSERT(err->message() == "read capacity unit must be positive.")
            (cu)(*err).issue();
    }
    {
        CapacityUnit cu;
        cu.mutableWrite() = -1;
        Optional<Error> err = cu.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (*err).issue();
        TESTA_ASSERT(err->message() == "write capacity unit must be positive.")
            (cu)(*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(CapacityUnit_validate);

void ColumnValue_none()
{
    ColumnValue v;
    TESTA_ASSERT(v.category() == ColumnValue::NONE)
        (v.category()).issue();
    TESTA_ASSERT(pp::prettyPrint(v) == "none")
        (v).issue();
    {
        Optional<Error> err = v.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (v)(*err).issue();
        TESTA_ASSERT(err->message() == "value is not set.")
            (v)(*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(ColumnValue_none);

void ColumnValue_int()
{
    ColumnValue v = ColumnValue::toInteger(1);
    TESTA_ASSERT(v.category() == ColumnValue::INTEGER)
        (v.category()).issue();
    TESTA_ASSERT(pp::prettyPrint(v) == "1")
        (v).issue();
    v.mutableInteger() = 2;
    TESTA_ASSERT(pp::prettyPrint(v) == "2")
        (v).issue();
}
TESTA_DEF_JUNIT_LIKE1(ColumnValue_int);

void ColumnValue_str()
{
    ColumnValue v = ColumnValue::toStr(MemPiece::from("a"));
    TESTA_ASSERT(v.category() == ColumnValue::STRING)
        (v.category()).issue();
    TESTA_ASSERT(pp::prettyPrint(v) == "\"a\"")
        (v).issue();
    v.mutableStr() = MemPiece::from("b");
    TESTA_ASSERT(pp::prettyPrint(v) == "\"b\"")
        (v).issue();
}
TESTA_DEF_JUNIT_LIKE1(ColumnValue_str);

void ColumnValue_blob()
{
    ColumnValue v = ColumnValue::toBlob(MemPiece::from("a"));
    TESTA_ASSERT(v.category() == ColumnValue::BINARY)
        (v.category()).issue();
    TESTA_ASSERT(pp::prettyPrint(v) == "b\"a\"")
        (v).issue();
    v.mutableBlob() = MemPiece::from("b");
    TESTA_ASSERT(pp::prettyPrint(v) == "b\"b\"")
        (v).issue();
}
TESTA_DEF_JUNIT_LIKE1(ColumnValue_blob);

void ColumnValue_bool()
{
    ColumnValue v = ColumnValue::toBoolean(true);
    TESTA_ASSERT(v.category() == ColumnValue::BOOLEAN)
        (v.category()).issue();
    TESTA_ASSERT(pp::prettyPrint(v) == "true")
        (v).issue();
    v.mutableBoolean() = false;
    TESTA_ASSERT(pp::prettyPrint(v) == "false")
        (v).issue();
}
TESTA_DEF_JUNIT_LIKE1(ColumnValue_bool);

void ColumnValue_floating()
{
    ColumnValue v = ColumnValue::toFloatPoint(1.2);
    TESTA_ASSERT(v.category() == ColumnValue::FLOATING_POINT)
        (v.category()).issue();
    TESTA_ASSERT(pp::prettyPrint(v) == "1.2000")
        (v).issue();
    v.mutableFloatPoint() = 2.1;
    TESTA_ASSERT(pp::prettyPrint(v) == "2.1000")
        (v).issue();
}
TESTA_DEF_JUNIT_LIKE1(ColumnValue_floating);

void Column_pp()
{
    Column v("attr", ColumnValue::toInteger(1), 2);
    TESTA_ASSERT(pp::prettyPrint(v) == "{\"Name\":\"attr\",\"Value\":1,\"Timestamp\":2}")
        (v).issue();
}
TESTA_DEF_JUNIT_LIKE1(Column_pp);

void Column_validate()
{
    {
        Column v("", ColumnValue::toInteger(1));
        Optional<Error> err = v.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (v)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (v)(*err).issue();
        TESTA_ASSERT(err->message() == "Column name must be nonempty.")
            (v)(*err).issue();
    }
    {
        Column v("attr", ColumnValue());
        Optional<Error> err = v.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (v)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (v)(*err).issue();
        TESTA_ASSERT(err->message() == "For column \"attr\", value is not set.")
            (v)(*err).issue();
    }
    {
        Column v("attr", ColumnValue::toInteger(1), -1);
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
TESTA_DEF_JUNIT_LIKE1(Column_validate);

void Row_pp()
{
    Row row;
    row.mutablePrimaryKey().append().mutableName() = "pkey";
    row.mutablePrimaryKey().back().mutableValue() = PrimaryKeyValue::toInteger(1);
    row.mutableAttributes().append().mutableName() = "attr";
    row.mutableAttributes().back().mutableValue() = ColumnValue::toInteger(2);
    TESTA_ASSERT(pp::prettyPrint(row) == "{\"PrimaryKey\":{\"pkey\":1},\"Attributes\":[{\"Name\":\"attr\",\"Value\":2}]}")
        (row).issue();
}
TESTA_DEF_JUNIT_LIKE1(Row_pp);

void Row_validate()
{
    Row row;
    {
        Optional<Error> err = row.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (row)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (row)(*err).issue();
        TESTA_ASSERT(err->message() == "Primary key must be nonempty.")
            (row)(*err).issue();
    }
    row.mutablePrimaryKey().append() = PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(1));
    row.mutableAttributes().append().mutableName() = "attr";
    {
        Optional<Error> err = row.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (row)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (row)(*err).issue();
        TESTA_ASSERT(err->message() == "For column \"attr\", value is not set.")
            (row)(*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(Row_validate);

void TimeRange_pp()
{
    TimeRange tr(UtcTime::fromMsec(1), UtcTime::fromMsec(2));
    TESTA_ASSERT(pp::prettyPrint(tr) == "[1970-01-01T00:00:00.001000Z,1970-01-01T00:00:00.002000Z)")
        (tr).issue();
}
TESTA_DEF_JUNIT_LIKE1(TimeRange_pp);

void TimeRange_validate()
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

void Split_pp()
{
    Split split;
    split.mutableLowerBound().reset(new PrimaryKey());
    split.mutableLowerBound()->append() = PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(1));
    split.mutableUpperBound().reset(new PrimaryKey());
    split.mutableUpperBound()->append() = PrimaryKeyColumn("pkey", PrimaryKeyValue::toInteger(2));
    split.mutableLocation() = "youguess";
    TESTA_ASSERT(pp::prettyPrint(split) == "{\"Location\":\"youguess\",\"LowerBound\":{\"pkey\":1},\"UpperBound\":{\"pkey\":2}}")
        (split).issue();
}
TESTA_DEF_JUNIT_LIKE1(Split_pp);

void Split_validate()
{
    {
        Split s;
        s.mutableLowerBound().reset(new PrimaryKey());
        s.mutableLowerBound()->append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(1));
        s.mutableUpperBound().reset(new PrimaryKey());
        s.mutableUpperBound()->append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(2));
        s.mutableUpperBound()->append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(3));
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
        s.mutableLowerBound().reset(new PrimaryKey());
        s.mutableLowerBound()->append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(1));
        s.mutableUpperBound().reset(new PrimaryKey());
        s.mutableUpperBound()->append() = PrimaryKeyColumn("pk1", PrimaryKeyValue::toInteger(2));
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
        s.mutableLowerBound().reset(new PrimaryKey());
        s.mutableLowerBound()->append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(1));
        s.mutableUpperBound().reset(new PrimaryKey());
        s.mutableUpperBound()->append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toStr(MemPiece::from("a")));
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
        s.mutableLowerBound().reset(new PrimaryKey());
        s.mutableLowerBound()->append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(1));
        s.mutableUpperBound().reset(new PrimaryKey());
        s.mutableUpperBound()->append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(0));
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
        s.mutableLowerBound().reset(new PrimaryKey());
        s.mutableLowerBound()->append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toAutoIncrement());
        s.mutableUpperBound().reset(new PrimaryKey());
        s.mutableUpperBound()->append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toAutoIncrement());
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

void CreateTableRequest_pp()
{
    CreateTableRequest req;
    req.mutableMeta().mutableTableName() = "table";
    req.mutableMeta().mutableSchema().append().mutableName() = "pkey";
    req.mutableMeta().mutableSchema().back().mutableType() = PKT_STRING;
    req.mutablePartitionSplitPoints().append().append() = PrimaryKeyColumn("pkey", PrimaryKeyValue::toStr(MemPiece::from("a")));
    TESTA_ASSERT(pp::prettyPrint(req) == "{\"Meta\":{\"TableName\":\"table\",\"Schema\":[{\"pkey\":PKT_STRING}]},"
        "\"Options\":{\"ReservedThroughput\":{\"Read\":0,\"Write\":0}},"
        "\"PartitionSplitPoints\":[{\"pkey\":\"a\"}]}")
        (req).issue();
}
TESTA_DEF_JUNIT_LIKE1(CreateTableRequest_pp);

void CreateTableRequest_validate()
{
    {
        CreateTableRequest req;
        req.mutableMeta().mutableTableName() = "table";
        req.mutableMeta().mutableSchema().append().mutableName() = "pk0";
        req.mutableMeta().mutableSchema().back().mutableType() = PKT_STRING;
        req.mutablePartitionSplitPoints().append().append() = PrimaryKeyColumn("pk1", PrimaryKeyValue::toStr(MemPiece::from("a")));
        Optional<Error> err = req.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (req)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (req)(*err).issue();
        TESTA_ASSERT(err->message() == "Partition split points contains \"pk1\", which is different with that in the schema.")
            (req)(*err).issue();
    }
    {
        CreateTableRequest req;
        req.mutableMeta().mutableTableName() = "table";
        req.mutableMeta().mutableSchema().append().mutableName() = "pk0";
        req.mutableMeta().mutableSchema().back().mutableType() = PKT_STRING;
        req.mutablePartitionSplitPoints().append().append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(1));
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
        req.mutableMeta().mutableTableName() = "table";
        req.mutableMeta().mutableSchema().append().mutableName() = "pk0";
        req.mutableMeta().mutableSchema().back().mutableType() = PKT_INTEGER;
        req.mutablePartitionSplitPoints().append().append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toInteger(1));
        req.mutablePartitionSplitPoints().back().append() = PrimaryKeyColumn("pk1", PrimaryKeyValue::toInteger(2));
        Optional<Error> err = req.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (req)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (req)(*err).issue();
        TESTA_ASSERT(err->message() == "Length of partition split points must be exactly one.")
            (req)(*err).issue();
    }
    {
        CreateTableRequest req;
        req.mutableMeta().mutableTableName() = "table";
        req.mutableMeta().mutableSchema().append().mutableName() = "pk0";
        req.mutableMeta().mutableSchema().back().mutableType() = PKT_INTEGER;
        req.mutablePartitionSplitPoints().append().append() = PrimaryKeyColumn("pk0", PrimaryKeyValue::toInfMin());
        Optional<Error> err = req.validate();
        TESTA_ASSERT(err.present()).issue();
        TESTA_ASSERT(err->httpStatus() < 0)
            (req)(*err).issue();
        TESTA_ASSERT(err->errorCode() == "OTSParameterInvalid")
            (req)(*err).issue();
        TESTA_ASSERT(err->message() == "Partition split points contains an unreal value type -INF.")
            (req)(*err).issue();
    }
}
TESTA_DEF_JUNIT_LIKE1(CreateTableRequest_validate);

} // namespace core
} // namespace tablestore
} // namespace aliyun

