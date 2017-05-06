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
#include "config.hpp"
#include "tablestore/core/types.hpp"
#include "tablestore/util/mempiece.hpp"
#include "tablestore/util/assert.hpp"
#include "tablestore/util/move.hpp"
#include "tablestore/util/foreach.hpp"
#include <tr1/functional>
#include <string>
#include <fstream>

using namespace std;
using namespace std::tr1;
using namespace std::tr1::placeholders;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

namespace {

struct State
{
    explicit State(Endpoint* ep, Credential* cr)
      : mEndpoint(ep),
        mCredential(cr)
    {
    }

    mutable Endpoint* mEndpoint;
    mutable Credential* mCredential;
    function<State(const State&, const string& line)> mFn;
};

bool isWhitespace(char c)
{
    return c == '\t' || c == ' ';
}

string trim(const string& in)
{
    const char* b = in.data();
    const char* e = b + in.size() - 1;
    for(; b <= e && isWhitespace(*b); ++b) {
    }
    for(; b <= e && isWhitespace(*e); --e) {
    }
    string res;
    for(; b <= e; ++b) {
        res.push_back(*b);
    }
    return res;
}

MemPiece extractTable(const MemPiece& line)
{
    if (line.startsWith(MemPiece::from("[")) && line.endsWith(MemPiece::from("]"))) {
        return line.subpiece(1, line.length() - 2);
    } else {
        return MemPiece();
    }
}

void extractKeyValue(MemPiece* key, MemPiece* val, const MemPiece& in)
{
    const uint8_t* b = in.data();
    const uint8_t* e = b + in.length();

    const uint8_t* i = b;
    for(; i < e && !isWhitespace((char) *i) && *i != '='; ++i) {
    }
    *key = MemPiece(b, i - b);
    for(; i < e && isWhitespace(*i); ++i) {
    }
    OTS_ASSERT(i < e)((uintptr_t) i)((uintptr_t) e);
    OTS_ASSERT(*i == '=')(*i);
    ++i;
    for(; i < e && isWhitespace(*i); ++i) {
    }
    *val = MemPiece(i, e - i);
}

State expectTable(const State& state, const string& line);
State fillEndpoint(const State& state, const string& line);
State fillCredential(const State& state, const string& line);

State fillEndpoint(const State& state, const string& line)
{
    string trimmed = trim(line);
    if (trimmed.empty()) {
        return state;
    }
    MemPiece table = extractTable(MemPiece::from(trimmed));
    if (table.length() > 0) {
        State nxtState = state;
        if (table == MemPiece::from("credential")) {
            nxtState.mFn = bind(fillCredential, _1, _2);
        } else {
            nxtState.mFn = bind(expectTable, _1, _2);
        }
        return nxtState;
    } else {
        MemPiece key;
        MemPiece val;
        extractKeyValue(&key, &val, MemPiece::from(trimmed));
        if (key == MemPiece::from("endpoint")) {
            *state.mEndpoint->mutableEndpoint() =
                val.subpiece(1, val.length() - 2).toStr();
        } else if (key == MemPiece::from("instance")) {
            *state.mEndpoint->mutableInstanceName() =
                val.subpiece(1, val.length() - 2).toStr();
        } else {
            OTS_ASSERT(false)(key.toStr())(val.toStr());
        }
        return state;
    }
}

State fillCredential(const State& state, const string& line)
{
    string trimmed = trim(line);
    if (trimmed.empty()) {
        return state;
    }
    MemPiece table = extractTable(MemPiece::from(trimmed));
    if (table.length() > 0) {
        State nxtState = state;
        nxtState.mFn = bind(expectTable, _1, _2);
        return nxtState;
    } else {
        MemPiece key;
        MemPiece val;
        extractKeyValue(&key, &val, MemPiece::from(trimmed));
        if (key == MemPiece::from("access-key-id")) {
            *state.mCredential->mutableAccessKeyId() =
                val.subpiece(1, val.length() - 2).toStr();
        } else if (key == MemPiece::from("access-key-secret")) {
            *state.mCredential->mutableAccessKeySecret() =
                val.subpiece(1, val.length() - 2).toStr();
        } else if (key == MemPiece::from("security-token")) {
            *state.mCredential->mutableSecurityToken() =
                val.subpiece(1, val.length() - 2).toStr();
        } else {
            OTS_ASSERT(false)(key.toStr())(val.toStr());
        }
        return state;
    }
}

State expectTable(const State& state, const string& line)
{
    string trimmed = trim(line);
    if (trimmed.empty()) {
        return state;
    }
    MemPiece table = extractTable(MemPiece::from(trimmed));
    if (table.length() > 0) {
        if (table == MemPiece::from("endpoint")) {
            State nxt = state;
            nxt.mFn = bind(fillEndpoint, _1, _2);
            return nxt;
        } else if (table == MemPiece::from("credential")) {
            State nxt = state;
            nxt.mFn = bind(fillCredential, _1, _2);
            return nxt;
        } else {
            return state;
        }
    } else {
        return state;
    }
}

} // namespace

void read(Endpoint* ep, Credential* cr)
{
    {
        Endpoint empty;
        moveAssign(ep, util::move(empty));
    }
    {
        Credential empty;
        moveAssign(cr, util::move(empty));
    }

    ifstream fin("config.toml");
    string line;
    State state(ep, cr);
    state.mFn = bind(expectTable, _1, _2);
    for(;;) {
        getline(fin, line);
        if (!fin) {
            break;
        }
        State nxtState = state.mFn(state, line);
        state = nxtState;
    }
    fin.close();
}

} // namespace core
} // namespace tablestore
} // namespace aliyun
