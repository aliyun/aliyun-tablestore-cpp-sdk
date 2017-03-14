#ifndef COMMON_UTIL_H
#define COMMON_UTIL_H

#include <algorithm>
#include <ctime>
#include <string>
#include <stdint.h>
#include <sys/types.h>

namespace aliyun {
namespace openservices {
namespace ots {

/**
 * Get current date and time. The output format is "%a,%d %b %Y %H:%M:%S GMT"
 * by default.
 * @param datetime The current data and time.
 */
void UTCTime2String(std::string* datetime);

/**
 * Get current date and time. The output format is "%a,%d %b %Y %H:%M:%S GMT"
 * by default.
 * @return The current date and time.
 */
std::string UTCTime2String();

/**
 * Get current time in seconds.
 * @return The local time in second; return -1 on error.
 */
int64_t UTCTimeInSecond();

/**
 * Convert a string, whose format is "%a,%d %b %Y %H:%M:%S GMT", to local time 
 * in seconds. If the format is invalid, return 0.
 * @return The seconds of the datetime.
 */
int64_t UTCString2LocalTime(const std::string& datetime);

/**
 * Convert a string to lower case.
 * @param value The original string.
 * @return The lower string.
 */
void StringToLower(const std::string& src, std::string* dest);

std::string StringToLower(const std::string& src);

/**
 * Trim the ' ', '\n' in a string.
 * @param value The original string.
 */
void StringTrim(std::string* value);

/**
 * Trim the ' ', '\n' in a string
 * @param value The original string.
 * @return The trimmed string without ' ' and '\n'.
 */
std::string StringTrim(std::string& value);

/**
 * Encode a string by Base64 algorithm.
 * @param plain The input string.
 * @param output The pointer of output string. 
 * @return true or false.
 */
bool Base64Encode(const std::string& plain, std::string* output);

/**
 * Generate the MD5 value for a plain string. The output is also a string. 
 * @param plain The plain string.
 * @param output The MD5 value.
 */
void MD5String(const std::string& plain, std::string* output);

/**
 * Generate the digest via HmacSha1.
 * @param key The key.
 * @param text The string to encypt.
 * @param digest The generated digest.
 */
void HmacSha1(const std::string& key, 
            const std::string& text, 
            std::string* digest);

/**
 * encode URL
 */
void EncodeURL(const std::string& url, std::string* output);

std::string EncodeURL(const std::string& url);

/**
 * Convert a string to an integer
 */
bool StringToInt64(const std::string& strValue, int64_t* value);

std::string Int64ToString(int64_t intValue);

/**
 * Compress via inflate algorithm
 */
bool DeflateCompress(const std::string& rawData, std::string* output);

bool DeflateCompress(const char* rawData, 
            uint64_t rawSize,
            char* output,
            uint64_t* outputSize);

/**
 * Decompress via infalte algorithm
 */
bool DeflateDecompress(const std::string& compData, 
            uint64_t rawSize,
            std::string *output);

bool DeflateDecompress(const char* compData, 
            uint64_t compSize, 
            char* output,
            uint64_t* outputSize);

} //end of ots
} //end of openservices
} //end of aliyun

#endif
