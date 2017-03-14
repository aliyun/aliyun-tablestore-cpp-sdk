#ifndef OTS_STATIC_INDEX_JSONIZE_H
#define OTS_STATIC_INDEX_JSONIZE_H

#include "string_tools.h"
#include "jsoncpp/json/value.h"

namespace static_index {

class Logger;

namespace details {

template<typename T>
class Jsonizer
{};

template<typename T>
class Unjsonizer
{};

} // namespace details

template<typename T>
Json::Value Jsonize(Logger* logger, const T& val)
{
    details::Jsonizer<T> jsonizer(logger);
    return jsonizer(val);
}

template<typename T>
void Unjsonize(Logger* logger, T* out, const ::Json::Value& val)
{
    details::Unjsonizer<T> unjsonizer(logger);
    unjsonizer(out, val);
}

namespace details {

template<>
class ConvertToStr<Json::Value>
{
public:
    ::std::string Convert(const Json::Value& x) const;
};

} // namespace details

} // namespace static_index

#endif /* OTS_STATIC_INDEX_JSONIZE_H */

