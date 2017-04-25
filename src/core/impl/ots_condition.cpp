/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
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
