#include "jsonize.h"
#include "jsoncpp/json/writer.h"

using namespace ::std;

namespace static_index {
namespace details {

string ConvertToStr<Json::Value>::Convert(const Json::Value& val) const
{
    Json::FastWriter writer;
    return writer.write(val);
}

} // namespace details
} // namespace static_index

