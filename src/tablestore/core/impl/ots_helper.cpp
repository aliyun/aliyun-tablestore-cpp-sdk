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
#include "ots_helper.hpp"
// #include "../plainbuffer/plain_buffer_consts.h"
#include <cstring>
#include <iostream>
#include <sstream>
#include <fstream>
extern "C" {
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/md5.h>
#include <openssl/sha.h>
#include <openssl/buffer.h>
#include <uuid/uuid.h>
#include <zlib.h>
}

using namespace std;

namespace aliyun {
namespace tablestore {
namespace core {

const size_t kChunkSize = 64*1024; 

string OTSHelper::UUIDString()
{
    static char charset[16] = {'0', '1', '2', '3', '4', '5', '6', '7',
                               '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    uuid_t uuid;
    uuid_generate(uuid);

    string uuidStr;
    uuidStr.reserve(36);    // 16*2(char) + 4('-')
    for (int i = 0; i < 16; ++i) {
        int lower = (int)(uuid[i] & 0xf); 
        int higher = (int)(uuid[i] >> 8);
        uuidStr.append(1, charset[lower]);
        uuidStr.append(1, charset[higher]);
        if (i == 3 || i == 5 || i == 7 || i == 9) {
            uuidStr.append(1, '-');
        }
    }
    return uuidStr;
}

int64_t OTSHelper::LocalTimeInSecond()
{
    return time(0);
}

int64_t OTSHelper::UTCString2LocalTime(const string& datetime)
{
    struct tm utcTime;
    char* p = strptime(datetime.c_str(), "%a, %d %b %Y %H:%M:%S GMT", &utcTime);
    return (p == NULL)? -1: (int64_t)timegm(&utcTime);
}

void OTSHelper::StringToLower(const string& src, string* dest)
{
    if (dest != NULL) {
        dest->resize(src.size());
        transform(src.begin(), src.end(), dest->begin(), ::tolower);
    }
}

string OTSHelper::StringToLower(const string& src)
{
    string dest;
    StringToLower(src, &dest);
    return dest;
}

void OTSHelper::StringTrim(string* value)
{
    if (value != NULL) {
        value->erase(0, value->find_first_not_of(" \r\n"));
        value->erase(value->find_last_not_of(" \r\n") + 1);
    }
}

string OTSHelper::StringTrim(string& value)
{
    string tmpValue = value;
    tmpValue.erase(0, tmpValue.find_first_not_of(" \r\n"));
    tmpValue.erase(tmpValue.find_last_not_of(" \r\n") + 1);
    return tmpValue;
}

bool OTSHelper::Base64Encode(const string& plain, string* output)
{
    if (output == NULL) {
        return false; 
    }

    int ret;
    BIO *bmem, *b64;
    b64 = BIO_new(BIO_f_base64());
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
    bmem = BIO_new(BIO_s_mem());    
    b64 =  BIO_push(b64, bmem);
    ret = BIO_write(b64, plain.c_str(), plain.size());
    if (ret < 0) {
        return false;
    }
    ret = BIO_flush(b64);
    if (ret < 0) {
        return false;
    }

    BUF_MEM* bptr;
    BIO_get_mem_ptr(b64, &bptr);
    output->assign(bptr->data, bptr->length);
    BIO_free_all(b64);
    return true;
}

bool OTSHelper::Base64Decode(const string& encode, string* output)
{
    if (output == NULL) {
        return false; 
    }

    BIO *b64, *bmem;
    b64 = BIO_new(BIO_f_base64());
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
    bmem = BIO_new_mem_buf((void*)encode.c_str(), encode.size());
    bmem = BIO_push(b64, bmem);

    int len = 0;
    u_char buff[1024*1024];
    len = BIO_read(bmem, buff, 1024*1024);
    output->assign((const char*)buff, len);
    BIO_free_all(bmem);
    return true;
}

void OTSHelper::MD5String(const string& plain, string* output)
{
    if (output == NULL) {
        return; 
    }
    u_char md[16];
    MD5((const u_char*)plain.c_str(), plain.size(), md);
    output->assign((char*)md, 16);
}

void OTSHelper::HmacSha1(const string& key, const string& text, string* digest)
{
    if (digest == NULL) {
        return; 
    }

    u_char md[20];
    u_char mdkey[20];
    u_char k_ipad[64], k_opad[64];
    u_int32_t i;

    const char* shaPtr = key.c_str();
    int shaLen = key.size();
    if (shaLen > 64) {
        SHA_CTX ctx;
        SHA1_Init(&ctx);
        SHA1_Update(&ctx, key.c_str(), key.size());
        SHA1_Final(mdkey, &ctx);
        shaLen = 20;
        shaPtr = (const char*)mdkey;
    }

    memcpy(k_ipad, shaPtr, shaLen);
    memcpy(k_opad, shaPtr, shaLen);
    memset(k_ipad + shaLen, 0, 64 - shaLen);
    memset(k_opad + shaLen, 0, 64 - shaLen);

    for (i = 0; i < 64; ++i) {
        k_ipad[i] ^= 0x36;
        k_opad[i] ^= 0x5c;
    }

    SHA_CTX ctx;
    SHA1_Init(&ctx);
    SHA1_Update(&ctx, k_ipad, 64);
    SHA1_Update(&ctx, text.c_str(), text.size());
    SHA1_Final(md, &ctx);

    SHA1_Init(&ctx);
    SHA1_Update(&ctx, k_opad, 64);
    SHA1_Update(&ctx, md, 20);
    SHA1_Final(md, &ctx);

    digest->assign((char*)md, 20);
}

char OTSHelper::Dec2HexChar(short int n) 
{
    if ( 0 <= n && n <= 9 ) {
        return char( short('0') + n );
    } else if ( 10 <= n && n <= 15 ) {
        return char( short('A') + n - 10 );
    } else {
        return char(0);
    }
}

int16_t OTSHelper::HexChar2Dec(char c)
{
    if ( '0'<=c && c<='9' ) {
        return short(c-'0');
    } else if ( 'a'<=c && c<='f' ) {
        return ( short(c-'a') + 10 );
    } else if ( 'A'<=c && c<='F' ) {
        return ( short(c-'A') + 10 );
    } else {
        return -1;
    }
}

void OTSHelper::EncodeURL(const string& url, string* output)
{
    if (output == NULL) {
        return;
    }

    output->clear();
    for ( size_t i=0; i < url.size(); ++i)
    {
        char c = url[i];
        if ( ( '0'<=c && c<='9' ) || ( 'a'<=c && c<='z' ) ||
             ( 'A'<=c && c<='Z' ) || c == '/' || c == '.') {
            *output += c;
        } else {
            int j = (short int)c;
            if ( j < 0 ) {
                j += 256;
            }
            int i1, i0;
            i1 = j / 16;
            i0 = j - i1*16;
            *output += '%';
            *output += Dec2HexChar(i1);
            *output += Dec2HexChar(i0);
        }
    }
}

string OTSHelper::EncodeURL(const string& url)
{
    string result;
    EncodeURL(url, &result);
    return result;
}

void OTSHelper::DecodeURL(const string& url, string* output)
{
    if (output == NULL) {
        return; 
    }

    for (size_t i = 0; i < url.size(); ++i) 
    {
        char c = url[i];
        if ( c != '%' ) {
            *output += c;
        } else {
            char c1 = url[++i];
            char c0 = url[++i];
            int num = 0;
            num += HexChar2Dec(c1) * 16 + HexChar2Dec(c0);
            *output += char(num);
        }
    }
}

string OTSHelper::DecodeURL(const string& url)
{
    string result;
    DecodeURL(url, &result);
    return result;
}

int64_t OTSHelper::StringToInt64(const string& strValue)
{
    return strtoll(strValue.c_str(), NULL, 10);
}

// string OTSHelper::TagTypeToString(int32_t tag)
// {
//     switch (tag) {
//     case TAG_ROW_PK:
//         return "TAG_ROW_PK";
//     case TAG_ROW_DATA:
//         return "TAG_ROW_DATA";
//     case TAG_CELL:
//         return "TAG_CELL";
//     case TAG_CELL_NAME:
//         return "TAG_CELL_NAME";
//     case TAG_CELL_VALUE:
//         return "TAG_CELL_VALUE";
//     case TAG_CELL_TYPE:
//         return "TAG_CELL_TYPE";
//     case TAG_CELL_TIMESTAMP:
//         return "TAG_CELL_TIMESTAMP";
//     case TAG_DELETE_ROW_MARKER:
//         return "TAG_DELETE_ROW_MARKER";
//     case TAG_ROW_CHECKSUM:
//         return "TAG_ROW_CHECKSUM";
//     case TAG_CELL_CHECKSUM:
//         return "TAG_CELL_CHECKSUM";
//     default:
//         return "UNKNOWN_TAG(" + OTSHelper::Int64ToString(tag) + ")";
//     }
// }

// string OTSHelper::VariantTypeToString(int32_t type)
// {
//     switch (type) {
//     case VT_INTEGER:
//         return "VT_INTEGER";
//     case VT_DOUBLE:
//         return "VT_DOUBLE";
//     case VT_BOOLEAN:
//         return "VT_BOOLEAN";
//     case VT_STRING:
//         return "VT_STRING";
//     case VT_NULL:
//         return "VT_NULL";
//     case VT_BLOB:
//         return "VT_BLOB";
//     case VT_INF_MIN:
//         return "VT_INF_MIN";
//     case VT_INF_MAX:
//         return "VT_INF_MAX";
//     default:
//         return "UNKNOWN_TYPE(" + OTSHelper::Int64ToString(type) + ")";
//     }
// }

// string OTSHelper::CompareOperatorToString(SingleColumnCondition::CompareOperator compareOperator)
// {
//     switch (compareOperator) {
//         case SingleColumnCondition::EQUAL:
//             return "EQUAL";
//         case SingleColumnCondition::NOT_EQUAL:
//             return "NOT_EQUAL";
//         case SingleColumnCondition::GREATER_THAN:
//             return "GREATER_EQUAL";
//         case SingleColumnCondition::GREATER_EQUAL:
//             return "GREATER_EQUAL";
//         case SingleColumnCondition::LESS_THAN:
//             return "LESS_THAN";
//         case SingleColumnCondition::LESS_EQUAL:
//             return "LESS_EQUAL";
//         default:
//             return "UNKNOWN_TYPE(" + OTSHelper::Int64ToString(compareOperator) + ")";
//     }
// }

// string OTSHelper::LogicOperatorToString(CompositeColumnCondition::LogicOperator logicOperator)
// {
//     switch (logicOperator) {
//     case CompositeColumnCondition::NOT:
//             return "NOT";
//         case CompositeColumnCondition::AND:
//             return "AND";
//         case CompositeColumnCondition::OR:
//             return "OR";
//         default:
//             return "UNKNOWN_TYPE(" + OTSHelper::Int64ToString(logicOperator) + ")";
//     }
// }

// string OTSHelper::ColumnConditionTypeToString(ColumnConditionType columnConditionType)
// {
//     switch (columnConditionType) {
//         case SINGLE_COLUMN_CONDITION:
//             return "SINGLE_COLUMN_CONDITION";
//         case COMPOSITE_COLUMN_CONDITION:
//             return "COMPOSITE_COLUMN_CONDITION";
//         default:
//             return "UNKNOWN_TYPE(" + OTSHelper::Int64ToString(columnConditionType) + ")";
//     }
// }

// string OTSHelper::DirectionToString(RangeRowQueryCriteria::Direction direction)
// {
//     switch (direction) {
//         case RangeRowQueryCriteria::FORWARD:
//             return "FORWARD";
//         case RangeRowQueryCriteria::BACKWARD:
//             return "BACKWARD";
//         default:
//             return "UNKNOWN_TYPE(" + OTSHelper::Int64ToString(direction) + ")";
//     }
// }

void OTSHelper::StringSplit(
    const string& value,
    const string& sep,
    vector<string>* subs)
{
    subs->clear();
    if (value.empty() || sep.empty()) {
        subs->push_back(value);
        return;
    }
    string::size_type pos1, pos2;
    pos2 = value.find(sep);
    pos1 = 0;
    while(string::npos != pos2)
    {   
        subs->push_back(value.substr(pos1, pos2 - pos1));

        pos1 = pos2 + sep.size();
        pos2 = value.find(sep, pos1);
    }   
    if(pos1 != value.length()) {
        subs->push_back(value.substr(pos1));
    }   
}

void OTSHelper::ParseEndpoint(
    const std::string& endpoint,
    std::vector<std::string>* output)
{
    string protocol;
    string address;
    string port;
    string tmpEndpoint; 

    output->clear();
    if (!endpoint.empty() && endpoint[endpoint.size() - 1] == '/') {
        tmpEndpoint = endpoint.substr(0, endpoint.size() - 1);
    } else {
        tmpEndpoint = endpoint;
    }

    vector<string> fields;
    string delim1 = "://";
    StringSplit(tmpEndpoint, delim1, &fields);
    if (fields.size() == 1) {
        protocol = "http";
        address = fields[0];
    } else if (fields.size() == 2) {
        if (fields[0] != "http" && fields[0] != "https") {
            return ;
        }
        protocol = fields[0];
        address = fields[1];
    } else {
        // invalid endpoint
        return;
    }
    
    string delim2 = ":";
    StringSplit(address, delim2, &fields);
    if (fields.size() == 1) {
        address = fields[0];
        if (protocol == "https") {
            port = "443"; 
        } else {
            port = "80";
        }
    } else if (fields.size() == 2) {
        address = fields[0];
        port = fields[1];
    } else {
        // invalid endpoint
        return;
    }

    if (address.empty()) {
        // invalid endpoint
        return;
    }
    output->push_back(protocol);
    output->push_back(address);
    output->push_back(port);
}

} // namespace core
} // namespace tablestore
} // namespace aliyun
