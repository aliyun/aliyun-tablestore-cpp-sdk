/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#ifndef OTS_HELPER_H
#define OTS_HELPER_H

#include "ots/ots_condition.h"
#include "ots/ots_request.h"
#include "ots/ots_types.h"
#include <algorithm>
#include <ctime>
#include <string>
#include <vector>
#include <stdint.h>

namespace aliyun {
namespace tablestore {

/**
 * @class OTSHelper
 * The common utils, including base64, md5 and hmacsha1.
 */
class OTSHelper
{
public:

    static std::string UUIDString();

    static std::string ISO8601TimeString();
    
    static int64_t LocalTimeInSecond();

    static int64_t UTCString2LocalTime(const std::string& datetime);

    static void StringToLower(const std::string& src, std::string* dest);

    static std::string StringToLower(const std::string& src);

    static void StringTrim(std::string* value);

    static void StringSplit(
        const std::string& value,
        const std::string& sep,
        std::vector<std::string>* subs);

    static void ParseEndpoint(
        const std::string& endpoint,
        std::vector<std::string>* output);

    static std::string StringTrim(std::string& value);

    static bool Base64Encode(const std::string& plain, std::string* output);

    static bool Base64Decode(const std::string& encode, std::string* output);

    static void MD5String(const std::string& plain, std::string* output);

    static void HmacSha1(const std::string& key, 
        const std::string& text, std::string* digest);

    /**
     * encode and decode URL
     */
    static char Dec2HexChar(short int n);

    static int16_t HexChar2Dec(char c);

    static void EncodeURL(const std::string& url, std::string* output);

    static std::string EncodeURL(const std::string& url);

    static void DecodeURL(const std::string& url, std::string* output);

    static std::string DecodeURL(const std::string& url);

    /**
     * Convert a string to an integer
     */
    static int64_t StringToInt64(const std::string& strValue);

    static std::string Int64ToString(int64_t intValue);

    static std::string BoolToString(bool value);

    /**
     * Convert enum type to string.
     */
    static std::string PrimaryKeyTypeToString(PrimaryKeyType type);

    static std::string ColumnTypeToString(ColumnType type);

    static std::string TagTypeToString(int32_t tag);

    static std::string VariantTypeToString(int32_t type);

    static std::string CompareOperatorToString(
            SingleColumnCondition::CompareOperator compareOperator);

    static std::string LogicOperatorToString(
            CompositeColumnCondition::LogicOperator logicOperator);

    static std::string ColumnConditionTypeToString(ColumnConditionType columnConditionType);

    static std::string DirectionToString(RangeRowQueryCriteria::Direction direction);

    static bool StartsWith(const std::string& str, const std::string& prefix);
};

} // end of tablestore
} // end of aliyun

#endif
