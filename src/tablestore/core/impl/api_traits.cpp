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
#include "api_traits.hpp"

using namespace std;

namespace aliyun {
namespace tablestore {
namespace core {
namespace impl {

const string ApiTraits<kApi_ListTable>::kPath = "/ListTable";
const string ApiTraits<kApi_CreateTable>::kPath = "/CreateTable";
const string ApiTraits<kApi_DeleteTable>::kPath = "/DeleteTable";
const string ApiTraits<kApi_DescribeTable>::kPath = "/DescribeTable";
const string ApiTraits<kApi_UpdateTable>::kPath = "/UpdateTable";
const string ApiTraits<kApi_ComputeSplitsBySize>::kPath = "/ComputeSplitPointsBySize";
const string ApiTraits<kApi_PutRow>::kPath = "/PutRow";
const string ApiTraits<kApi_GetRow>::kPath = "/GetRow";
const string ApiTraits<kApi_GetRange>::kPath = "/GetRange";
const string ApiTraits<kApi_UpdateRow>::kPath = "/UpdateRow";
const string ApiTraits<kApi_DeleteRow>::kPath = "/DeleteRow";
const string ApiTraits<kApi_BatchGetRow>::kPath = "/BatchGetRow";
const string ApiTraits<kApi_BatchWriteRow>::kPath = "/BatchWriteRow";

} // namespace impl
} // namespace core
} // namespace tablestore
} // namespace aliyun

