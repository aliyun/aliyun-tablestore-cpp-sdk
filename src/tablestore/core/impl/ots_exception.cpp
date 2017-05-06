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
#include "ots_exception.hpp"
#include "tablestore/util/prettyprint.hpp"

using namespace std;

namespace aliyun {
namespace tablestore {

// OTSClientException

OTSClientException::OTSClientException(const string& message)
    : mMessage(message)
{
    mWhat = "Message: " + mMessage;
}

OTSClientException::OTSClientException(
    const string& message,
    const string& traceId)
    : mMessage(message)
    , mTraceId(traceId)
{
    mWhat = "Message: " + mMessage;
    if (!mTraceId.empty()) {
        mWhat += ", TraceId: " + mTraceId;
    }
}

OTSClientException::~OTSClientException() throw()
{
}

const char* OTSClientException::what() const throw()
{
    return mWhat.c_str();
}

string OTSClientException::GetMessage() const
{
    return mMessage;
}

string OTSClientException::GetTraceId() const
{
    return mTraceId;
}

} // end of tablestore
} // end of aliyun
