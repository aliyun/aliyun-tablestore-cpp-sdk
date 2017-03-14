#include "string_tools.h"
#include <sstream>

using namespace ::std;

namespace static_index {

namespace details {

const string ConvertToStr<bool>::kTrue("true");
const string ConvertToStr<bool>::kFalse("false");

string ConvertToStr<double>::Convert(double x) const
{
    ostringstream oss;
    oss << x;
    return oss.str();
}

} // namespace details

} // namespace static_index

