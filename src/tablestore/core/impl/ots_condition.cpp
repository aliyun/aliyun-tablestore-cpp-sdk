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
#include "ots_condition.hpp"
#include "ots_exception.hpp"
#include "../protocol/table_store_filter.pb.h"
#include "ots_protocol_builder.hpp"

using namespace std;
using namespace com::aliyun::tablestore;

namespace aliyun {
namespace tablestore {

// CompositeColumnCondition

CompositeColumnCondition::CompositeColumnCondition(
        CompositeColumnCondition::LogicOperator logicOperator)
    : mLogicOperator(logicOperator)
{
}

CompositeColumnCondition::~CompositeColumnCondition()
{
}

CompositeColumnCondition::LogicOperator CompositeColumnCondition::GetLogicOperator() const
{
    return mLogicOperator;
}

void CompositeColumnCondition::SetLogicOperator(
        CompositeColumnCondition::LogicOperator logicOperator)
{
    mLogicOperator = logicOperator;
}

const list<ColumnConditionPtr>& CompositeColumnCondition::GetSubConditions() const
{
    return mConditions;
}

void CompositeColumnCondition::AddColumnCondition(const ColumnConditionPtr& conditionPtr)
{
    mConditions.push_back(conditionPtr);
}

ColumnConditionType CompositeColumnCondition::GetColumnConditionType() const
{
    return COMPOSITE_COLUMN_CONDITION;;
}

// string CompositeColumnCondition::Serialize() const
// {
//     return OTSProtocolBuilder::ToCompositeCondition(this);
// }

// SingleColumnCondition

// SingleColumnCondition::SingleColumnCondition(
//     const string& columnName,
//     CompareOperator compareOperator,
//     const ColumnValue& columnValue)
//     : mColumnName(columnName)
//     , mCompareOperator(compareOperator)
//     , mColumnValue(columnValue)
//     , mPassIfMissing(true)
//     , mLatestVersionsOnly(true)
// {
// }

// SingleColumnCondition::~SingleColumnCondition()
// {
// }

// const string& SingleColumnCondition::GetColumnName() const
// {
//     return mColumnName;
// }

// void SingleColumnCondition::SetColumnName(const string& columnName)
// {
//     mColumnName = columnName;
// }

// SingleColumnCondition::CompareOperator SingleColumnCondition::GetCompareOperator() const
// {
//     return mCompareOperator;
// }

// void SingleColumnCondition::SetCompareOperator(
//         SingleColumnCondition::CompareOperator compareOperator)
// {
//     mCompareOperator = compareOperator;
// }

// const ColumnValue& SingleColumnCondition::GetColumnValue() const
// {
//     return mColumnValue;
// }

// void SingleColumnCondition::SetColumnValue(const ColumnValue& columnValue)
// {
//     mColumnValue = columnValue;
// }

// bool SingleColumnCondition::GetPassIfMissing() const
// {
//     return mPassIfMissing;
// }

// void SingleColumnCondition::SetPassIfMissing(bool passIfMissing)
// {
//     mPassIfMissing = passIfMissing;
// }

// bool SingleColumnCondition::GetLatestVersionsOnly() const
// {
//     return mLatestVersionsOnly;
// }

// void SingleColumnCondition::SetLatestVersionsOnly(bool latestVersionsOnly)
// {
//     mLatestVersionsOnly = latestVersionsOnly;
// }

// ColumnConditionType SingleColumnCondition::GetColumnConditionType() const
// {
//     return SINGLE_COLUMN_CONDITION;
// }

// string SingleColumnCondition::Serialize() const
// {
//     return OTSProtocolBuilder::ToSingleCondition(this);
// }

// Condition
Condition::Condition()
    : mRowCondition(IGNORE)
{
}


Condition::Condition(RowExistenceExpectation type)
    : mRowCondition(type)
{
}

Condition::Condition(RowExistenceExpectation type,
                     const ColumnConditionPtr &columnConditionPtr)
    : mRowCondition(type)
    , mColumnConditionPtr(columnConditionPtr)
{
}

void Condition::SetRowCondition(RowExistenceExpectation rowCondition)
{
    mRowCondition = rowCondition;
}

RowExistenceExpectation Condition::GetRowCondition() const
{
    return mRowCondition;
}

void Condition::SetColumnCondition(const ColumnConditionPtr &columnConditionPtr)
{
    mColumnConditionPtr = columnConditionPtr;
}

const ColumnConditionPtr& Condition::GetColumnCondition() const
{
    return mColumnConditionPtr;
}


} // end of tablestore
} // end of aliyun
