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

#include "testa.ipp"
#include <cstdlib>
#include <cstdio>

using namespace std;
#if __cplusplus < 201103L
using namespace std::tr1;
#endif

namespace {

void printUsage(string exe)
{
    printf("%s [--help|-h] [--show-cases] [CASENAME]\n", exe.c_str());
    printf("CASENAME\ta case name that will be executed\n");
    printf("--show-cases\ta list of case names, one name per line\n");
    printf("--help,-h\tthis help message\n");
}

} // namespace

int main(int argv, char** args)
{
    if (argv < 1) {
        abort();
    }
    string exe = args[0];
    if (argv != 2) {
        printUsage(exe);
        return 1;
    }
    string act = args[1];
    if (act == "--help" || act == "-h") {
        printUsage(exe);
        return 0;
    } else if (act == "--show-cases") {
        shared_ptr<testa::CaseMap> caseMap = testa::getCaseMap();
        for(testa::CaseMap::const_iterator i = caseMap->begin();
            i != caseMap->end();
            ++i)
        {
            printf("%s\n", i->first.c_str());
        }
        return 0;
    } else {
        shared_ptr<testa::CaseMap> caseMap = testa::getCaseMap();
        testa::CaseMap::const_iterator cit = caseMap->find(act);
        if (cit == caseMap->end()) {
            abort();
        }
        cit->second();
        return 0;
    }
}


