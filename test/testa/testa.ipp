#pragma once
/*
This file is picked from project testa [https://github.com/TimeExceed/testa.git]
Copyright (c) 2017, Taoda (tyf00@aliyun.com)
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice, this
  list of conditions and the following disclaimer in the documentation and/or
  other materials provided with the distribution.

* Neither the name of the {organization} nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
#include "tablestore/util/prettyprint.hpp"

#if __cplusplus < 201103L
#include <tr1/memory>
#include <tr1/functional>
#include <tr1/tuple>
#else
#include <memory>
#include <functional>
#include <tuple>
#endif

#include <deque>
#include <map>
#include <string>

namespace testa {

#if __cplusplus < 201103L
typedef ::std::map<std::string, std::tr1::function<void()> > CaseMap;
std::tr1::shared_ptr<CaseMap> getCaseMap();
#else
typedef ::std::map<std::string, std::function<void()> > CaseMap;
std::shared_ptr<CaseMap> getCaseMap();
#endif

class CaseFailIssuer
{
public:
    explicit CaseFailIssuer(bool disable, const char* cond, const char* fn, int line);
    ~CaseFailIssuer();

    CaseFailIssuer& append(const std::string& key, const std::string& value);
    void issue(const std::string& msg);
    void issue();

private:
#if __cplusplus < 201103L
    typedef std::tr1::tuple<std::string, std::string> KV;
#else
    typedef std::tuple<std::string, std::string> KV;
#endif

    bool mTrigger;
    std::string mCondition;
    std::string mFilename;
    int mLine;
    std::deque<KV> mKeyValues;
    bool mIssued;

public:
    CaseFailIssuer& TESTA_PINGPONG_A;
    CaseFailIssuer& TESTA_PINGPONG_B;
};

class EqCase
{
public:
    template<class Res, class T>
    EqCase(
        const std::string& caseName,
#if __cplusplus < 201103L
        void (*tb)(const std::string&, std::tr1::function<void(const T&)>),
#else
        void (*tb)(const std::string&, std::function<void(const T&)>),
#endif
        Res (*trialFn)(const T&),
        Res (*oracleFn)(const T&))
    {
#if __cplusplus < 201103L
        std::tr1::function<void(T)> cs = std::tr1::bind(
#else
        std::function<void(T)> cs = std::bind(
#endif
            &EqCase::eq<Res, T>,
            trialFn,
            oracleFn,
#if __cplusplus < 201103L
            std::tr1::placeholders::_1);
        (*testa::getCaseMap())[caseName] = std::tr1::bind(tb, caseName, cs);
#else
            std::placeholders::_1);
        (*testa::getCaseMap())[caseName] = std::bind(tb, caseName, cs);
#endif
    }

    template<class Res, class T>
    EqCase(
        const std::string& caseName,
        Res (*trialFn)(const T&),
        Res (*oracleFn)(const T&),
        const T& in)
    {
#if __cplusplus < 201103L
        std::tr1::function<void()> cs = std::tr1::bind(
#else
        std::function<void()> cs = std::bind(
#endif
            &EqCase::eq<Res, T>,
            trialFn,
            oracleFn,
            in);
        (*testa::getCaseMap())[caseName] = cs;
    }

    template<class Res, class T0, class T1>
    EqCase(
        const std::string& caseName,
#if __cplusplus < 201103L
        Res (*trialFn)(const std::tr1::tuple<T0, T1>&),
        Res (*oracleFn)(const std::tr1::tuple<T0, T1>&),
#else
        Res (*trialFn)(const std::tuple<T0, T1>&),
        Res (*oracleFn)(const std::tuple<T0, T1>&),
#endif
        const T0& in0,
        const T1& in1)
    {
#if __cplusplus < 201103L
        std::tr1::function<void()> cs = std::tr1::bind(
            &EqCase::eq<Res, std::tr1::tuple<T0, T1> >,
#else
        std::function<void()> cs = std::bind(
            &EqCase::eq<Res, std::tuple<T0, T1> >,
#endif
            trialFn,
            oracleFn,
#if __cplusplus < 201103L
            std::tr1::make_tuple(in0, in1));
#else
            std::make_tuple(in0, in1));
#endif
        (*testa::getCaseMap())[caseName] = cs;
    }

    template<class Res, class T0, class T1, class T2>
    EqCase(
        const std::string& caseName,
#if __cplusplus < 201103L
        Res (*trialFn)(const std::tr1::tuple<T0, T1, T2>&),
        Res (*oracleFn)(const std::tr1::tuple<T0, T1, T2>&),
#else
        Res (*trialFn)(const std::tuple<T0, T1, T2>&),
        Res (*oracleFn)(const std::tuple<T0, T1, T2>&),
#endif
        const T0& in0,
        const T1& in1,
        const T2& in2)
    {
#if __cplusplus < 201103L
        std::tr1::function<void()> cs = std::tr1::bind(
            &EqCase::eq<Res, std::tr1::tuple<T0, T1, T2> >,
#else
        std::function<void()> cs = std::bind(
            &EqCase::eq<Res, std::tuple<T0, T1, T2> >,
#endif
            trialFn,
            oracleFn,
#if __cplusplus < 201103L
            std::tr1::make_tuple(in0, in1, in2));
#else
            std::make_tuple(in0, in1, in2));
#endif
        (*testa::getCaseMap())[caseName] = cs;
    }

private:
    template<class Res, class T>
    static void eq(
        Res (*trialFn)(const T&),
        Res (*oracleFn)(const T&),
        const T& in)
    {
        const Res& trialResult = trialFn(in);
        const Res& oracleResult = oracleFn(in);
        CaseFailIssuer issr(trialResult == oracleResult,
            "trialResult == oracleResult",
            __FILE__, __LINE__);
        issr.append("input", pp::prettyPrint(in))
            .append("trialResult", pp::prettyPrint(trialResult))
            .append("oracleResult", pp::prettyPrint(oracleResult))
            .issue();
    }
};

class VerifyCase
{
public:
    template<class Res, class T>
    VerifyCase(
        const std::string& caseName,
#if __cplusplus < 201103L
        void (*tb)(const std::string&, std::tr1::function<void(const T&)>),
#else
        void (*tb)(const std::string&, std::function<void(const T&)>),
#endif
        void (*verifier)(const Res&, const T&),
        Res (*trialFn)(const T&))
    {
#if __cplusplus < 201103L
        std::tr1::function<void(T)> cs = std::tr1::bind(
#else
        std::function<void(T)> cs = std::bind(
#endif
            &VerifyCase::verify<Res, T>,
            verifier,
            trialFn,
#if __cplusplus < 201103L
            std::tr1::placeholders::_1);
        (*testa::getCaseMap())[caseName] = std::tr1::bind(tb, caseName, cs);
#else
            std::placeholders::_1);
        (*testa::getCaseMap())[caseName] = std::bind(tb, caseName, cs);
#endif
    }

    VerifyCase(
        const std::string& caseName,
        void (*tbVerifier)(const std::string&))
    {
#if __cplusplus < 201103L
        (*testa::getCaseMap())[caseName] = std::tr1::bind(tbVerifier, caseName);
#else
        (*testa::getCaseMap())[caseName] = std::bind(tbVerifier, caseName);
#endif
    }

private:
    template<class Res, class T>
    static void verify(
        void (*verifier)(const Res&, const T&),
        Res (*trialFn)(const T&),
        const T& in)
    {
        const Res& res = trialFn(in);
        verifier(res, in);
    }
};

} // namespace testa

