#include "common_util.h"

#include <iostream>
#include <cstring>
#include <sstream>
#include <fstream>

extern "C" {
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/md5.h>
#include <openssl/sha.h>
#include <openssl/buffer.h>
#include <zlib.h>
}

using namespace std;

namespace aliyun {
namespace openservices {
namespace ots {

const size_t kChunkSize = 64*1024; 

void UTCTime2String(string* datetime)
{
    if (datetime != NULL) {
        tm result;
        char tmp[128] = {0};
        time_t t = time(0);
        gmtime_r(&t, &result);
        strftime(tmp, sizeof(tmp), "%a, %d %b %Y %H:%M:%S GMT", &result);
        *datetime = tmp;
    }
}

string UTCTime2String()
{
    string datetime;
    UTCTime2String(&datetime);
    return datetime;
}

//Return -1 on error.
int64_t UTCTimeInSecond()
{
    return time(0);
}

int64_t UTCString2LocalTime(const string& datetime)
{
    struct tm utcTime;
    char* p = strptime(datetime.c_str(), "%a, %d %b %Y %H:%M:%S GMT", &utcTime);
    return (p == NULL)? -1: (int64_t)timegm(&utcTime);
}

int CharToLower(int ch)
{
    return (ch >= 'A' && ch <= 'Z') ? ch + 32 : ch;
}

void StringToLower(const string& src, string* dest)
{
    if (dest != NULL) {
        dest->resize(src.size());
        transform(src.begin(), src.end(), dest->begin(), CharToLower);
    }
}

string StringToLower(const string& src)
{
    string dest;
    StringToLower(src, &dest);
    return dest;
}

void StringTrim(string* value)
{
    if (value != NULL) {
        size_t beginPos = value->find_first_not_of(" \r\n");
        if (string::npos == beginPos) {
            beginPos = 0;
        }
        size_t endPos = value->find_last_not_of(" \r\n");
        if (string::npos == endPos) {
            endPos = value->size() - 1;
        }
        size_t len = endPos - beginPos + 1;
        if (len < value->size()) {
            string tmpValue = value->substr(beginPos, len);
            value->swap(tmpValue);
        }
    }
}

string StringTrim(string& value)
{
    size_t beginPos = value.find_first_not_of(" \r\n");
    if (string::npos == beginPos) {
        beginPos = 0;
    }
    size_t endPos = value.find_last_not_of(" \r\n");
    if (string::npos == endPos) {
        endPos = value.size() - 1;
    }
    size_t len = endPos - beginPos + 1;
    if (len < value.size()) {
        return value.substr(beginPos, len);
    } else {
        return value;
    }
}

bool Base64Encode(const string& plain, string* output)
{
    if (output == NULL) {
        return false; 
    }

    int ret;
    BIO *bmem, *b64;
    b64 = BIO_new(BIO_f_base64());
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
    bmem = BIO_new(BIO_s_mem());    
    b64 = BIO_push(b64, bmem);
    ret = BIO_write(b64, plain.c_str(), plain.size());
    if (ret < 0) {
        BIO_free_all(b64);
        return false;
    }
    ret = BIO_flush(b64);
    if (ret < 0) {
        BIO_free_all(b64);
        return false;
    }

    BUF_MEM* bptr;
    BIO_get_mem_ptr(b64, &bptr);
    output->assign(bptr->data, bptr->length);
    BIO_free_all(b64);
    return true;
}

void MD5String(const string& plain, string* output)
{
    if (output == NULL) {
        return; 
    }
    u_char md[16];
    MD5((const u_char*)plain.c_str(), plain.size(), md);
    output->assign((char*)md, 16);
}

void HmacSha1(const string& key, const string& text, string* digest)
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

char Dec2HexChar(short int n) 
{
    if ( 0 <= n && n <= 9 ) {
        return char( short('0') + n );
    } else if ( 10 <= n && n <= 15 ) {
        return char( short('A') + n - 10 );
    } else {
        return char(0);
    }
}

int16_t HexChar2Dec(char c)
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

void EncodeURL(const string& url, string* output)
{
    if (output == NULL) {
        return;
    }

    output->clear();
    output->reserve(url.size());
    for (size_t i = 0; i < url.size(); ++i) {
        char c = url[i];
        if ( ('0' <= c && c <= '9') || ('a' <= c && c<='z') ||
             ('A' <= c && c <= 'Z') || c == '/' || c == '.') {
            *output += c;
        } else {
            int j = c;
            if ( j < 0 ) {
                j += 256;
            }
            int i1, i0;
            i1 = j / 16;
            i0 = j % 16;
            *output += '%';
            *output += Dec2HexChar(i1);
            *output += Dec2HexChar(i0);
        }
    }
}

string EncodeURL(const string& url)
{
    string result;
    EncodeURL(url, &result);
    return result;
}

bool StringToInt64(const string& strValue, int64_t* value)
{
    if (value == NULL) {
        return false;
    }
    char* endptr = NULL;
    int64_t intValue = strtoll(strValue.c_str(), &endptr, 10);
    if (endptr != (char*)(strValue.data() + strValue.size())) {
        return false;
    } else {
        *value = intValue;
        return true;
    }
}

string Int64ToString(int64_t intValue)
{
    char buf[64];
    snprintf(buf, sizeof(buf), "%lld", (long long int)intValue);
    return string(buf);
}

bool DeflateCompress(const string& rawData, string* output)
{
    output->clear();
    uint32_t rawLen = rawData.size();
    uLongf compLen = compressBound((uLong)rawLen);
    output->resize(compLen);
    Bytef* dest = (Bytef*)output->data();
    int32_t ret = compress(dest, &compLen, (const Bytef*)rawData.c_str(), (uLong)rawLen);
    output->resize(compLen);
    return (ret != Z_OK)? false: true;
}

bool DeflateCompress(const char* rawData, 
                uint64_t rawSize,
                char* output,
                uint64_t* outputSize)
{
    int32_t ret = compress((Bytef*)output, outputSize, (const Bytef*)rawData, rawSize);
    return (ret != Z_OK)? false: true;
}

bool DeflateDecompress(const string& compData, uint64_t rawSize, string *output)
{
    output->clear();
    output->resize(rawSize);
    int32_t ret = uncompress((Bytef*)output->data(), (uLong*)&rawSize, 
                (const Bytef*)compData.c_str(), compData.size());
    output->resize(rawSize); 
    return (ret != Z_OK)? false: true;
}


bool DeflateDecompress(const char* compData, 
                uint64_t compSize, 
                char* output,
                uint64_t* outputSize)
{
    int32_t ret = uncompress((Bytef*)output, (uLong*)outputSize, (const Bytef*)compData, compSize); 
    return (ret != Z_OK)? false: true;
}

} //end of ots
} //end of openservices
} //end of aliyun
