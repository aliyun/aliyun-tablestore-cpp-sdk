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

#include "util/metaprogramming.hpp"
#include <tr1/type_traits>
#include <cstring>

namespace aliyun {
namespace tablestore {
namespace util {

namespace impl {

template<>
struct ToMemPiece<std::string, void>
{
    MemPiece operator()(const std::string& x) const
    {
        return MemPiece(x.data(), x.size());
    }
};

template<>
struct ToMemPiece<const std::string, void>
{
    MemPiece operator()(const std::string& x) const
    {
        ToMemPiece<std::string, void> p;
        return p(x);
    }
};

template<int N>
struct ToMemPiece<char[N], void>
{
    MemPiece operator()(const char* x) const
    {
        return MemPiece(x, N - 1); // trim tailing '\0'
    }
};

template<int N>
struct ToMemPiece<const char[N], void>
{
    MemPiece operator()(const char* x) const
    {
        ToMemPiece<char[N], void> p;
        return p(x);
    }
};

template<>
struct ToMemPiece<char*, void>
{
    MemPiece operator()(const char* x) const
    {
        return MemPiece(x, ::strlen(x));
    }
};

template<>
struct ToMemPiece<const char*, void>
{
    MemPiece operator()(const char* x) const
    {
        ToMemPiece<char*, void> p;
        return p(x);
    }
};

} // namspace impl
} // namespace util
} // namespace tablestore
} // namespace aliyun
