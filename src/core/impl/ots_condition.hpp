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
#pragma once

#include "core/types.hpp"
#include <string>
#include <list>
#include <tr1/memory>

namespace aliyun {
namespace tablestore {

/**
 * @brief RowExistenceExpectation
 *
 * 行条件的类型。
 */
enum RowExistenceExpectation
{
    IGNORE           = 0,
    EXPECT_EXIST     = 1,
    EXPECT_NOT_EXIST = 2
};

/**
 * @brief ColumnConditionType
 *
 * 列条件类型：单条件和组合条件
 */
enum ColumnConditionType {
    SINGLE_COLUMN_CONDITION     = 1,
    COMPOSITE_COLUMN_CONDITION  = 2
};

/**
 * @brief ColumnCondition
 *
 * 列条件基类
 */
class ColumnCondition
{
public:
    ColumnCondition() {}
    virtual ~ColumnCondition() {}
    
    virtual ColumnConditionType GetColumnConditionType() const = 0;
    virtual std::string Serialize() const = 0;
};

typedef std::tr1::shared_ptr<ColumnCondition> ColumnConditionPtr;

/**
 * @brief SingleColumnCondition
 *
 * 单列条件
 */ 
class SingleColumnCondition : public ColumnCondition
{
public:
    enum CompareOperator {
        EQUAL          = 1, 
        NOT_EQUAL      = 2, 
        GREATER_THAN   = 3, 
        GREATER_EQUAL  = 4, 
        LESS_THAN      = 5, 
        LESS_EQUAL     = 6
    };
public:
//     SingleColumnCondition(const std::string& columnName,
//                           CompareOperator compareOperator,
//                           const ColumnValue& columnValue);
//     virtual ~SingleColumnCondition();
    
//     virtual ColumnConditionType GetColumnConditionType() const;
//     virtual std::string Serialize() const;

//     void SetColumnName(const std::string &columnName);
//     const std::string& GetColumnName() const;

//     void SetCompareOperator(CompareOperator compareOperator);
//     CompareOperator GetCompareOperator() const;

//     void SetColumnValue(const ColumnValue &columneValue);
//     const ColumnValue& GetColumnValue() const;

//     void SetPassIfMissing(bool passIfMissing);
//     bool GetPassIfMissing() const;

//     void SetLatestVersionsOnly(bool latestVersionsOnly);
//     bool GetLatestVersionsOnly() const;

// private:
//     std::string mColumnName;
//     CompareOperator mCompareOperator;
//     ColumnValue mColumnValue;
//     bool mPassIfMissing;
//     bool mLatestVersionsOnly;
};

/**
 * @brief CompositeColumnCondition
 *
 * 单列条件
 */ 
class CompositeColumnCondition : public ColumnCondition
{
public:
    enum LogicOperator {
        NOT = 1,
        AND = 2, 
        OR  = 3
    };
public:
    CompositeColumnCondition(LogicOperator logicOperator);
    virtual ~CompositeColumnCondition();

    virtual ColumnConditionType GetColumnConditionType() const;
    virtual std::string Serialize() const;
    
    void AddColumnCondition(const ColumnConditionPtr &condition);
    
    void SetLogicOperator(LogicOperator logicOperator);
    LogicOperator GetLogicOperator() const;

    const std::list<ColumnConditionPtr>& GetSubConditions() const;
private:
    LogicOperator mLogicOperator;
    std::list<ColumnConditionPtr> mConditions;
};

/**
 * @brief Condition
 *
 * 条件，用于条件更新或者过滤。
 */
class Condition
{
public:
    Condition();

    Condition(RowExistenceExpectation rowCondition);

    Condition(RowExistenceExpectation rowCondition,
              const ColumnConditionPtr& columnCondition);

    void SetRowCondition(RowExistenceExpectation rowCondition);
    RowExistenceExpectation GetRowCondition() const;

    void SetColumnCondition(const ColumnConditionPtr &columnConditionPtr);
    const ColumnConditionPtr& GetColumnCondition() const;
private:
    RowExistenceExpectation mRowCondition;
    ColumnConditionPtr mColumnConditionPtr;
};

} // end of tablestore
} // end of aliyun

