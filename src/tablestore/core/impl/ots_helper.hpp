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

#include <algorithm>
#include <ctime>
#include <string>
#include <vector>
#include <stdint.h>

namespace aliyun {
namespace tablestore {
namespace core {

/**
 * @class OTSHelper
 * The common utils, including base64, md5 and hmacsha1.
 */
class OTSHelper
{
public:

    static std::string UUIDString();

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

    /**
     * Convert enum type to string.
     */
    // static std::string TagTypeToString(int32_t tag);

    // static std::string VariantTypeToString(int32_t type);

    // static std::string CompareOperatorToString(
    //         SingleColumnCondition::CompareOperator compareOperator);

    // static std::string LogicOperatorToString(
    //         CompositeColumnCondition::LogicOperator logicOperator);

    // static std::string ColumnConditionTypeToString(ColumnConditionType columnConditionType);

    // static std::string DirectionToString(RangeRowQueryCriteria::Direction direction);

};

} // namespace core
} // namespace tablestore
} // namespace aliyun

