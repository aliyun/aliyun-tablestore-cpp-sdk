#!/usr/bin/python2.7 -B

# This file is picked from project testa [https://github.com/TimeExceed/testa.git]
# Copyright (c) 2013, Taoda (tyf00@aliyun.com)
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice, this
#   list of conditions and the following disclaimer in the documentation and/or
#   other materials provided with the distribution.
#
# * Neither the name of the {organization} nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import sys
import os
import os.path as op
from Queue import Queue
import threading
import json
import subprocess as subprocess
import shlex
import re
from functools import partial
from itertools import groupby
from datetime import datetime
import argparse
import termcolor

def countCpu():
    try:
        num = os.sysconf('SC_NPROCESSORS_ONLN')
        return num
    except (ValueError, OSError, AttributeError):
        raise NotImplementedError('cannot determine number of cpus')

def parseArgs():
    parser = argparse.ArgumentParser(description='Run some test cases, which obey testa protocol')
    parser.add_argument('executables', metavar='program', type=str, nargs='+',
                        help='executables obey testa protocol')
    parser.add_argument('-l', '--lang', nargs='?', default='lang.config',
                        help='a file specifying how to run executable for different programming languages. [default: lang.config]')
    parser.add_argument('-d', '--dir', nargs='?', default='test_results',
                        help='the directory where results of test cases are put [default: test_results]')
    parser.add_argument('-j', '--jobs', nargs='?', type=int, default=countCpu(),
                        help='how many test cases can run parallelly [default: as many as CPU cores]')
    parser.add_argument('-i', '--include', nargs='?', default='.*',
                        help='A regular expression. Only test cases matching this pattern will be run. [default: ".*"]')
    parser.add_argument('-e', '--exclude', nargs='?', default='^$',
                        help='A regular expression. Test cases matching this pattern will not be run. [default: "^$"]')
    parser.add_argument('--timeout', nargs='?',
                        help='how long a case is allowed to run (in msec) [default: disable]')
    parser.add_argument('--report', nargs='?',
                        help='report as a json file')
    args = parser.parse_args()
    return args

def readLangCfg(fn):
    with open(fn) as f:
        cfg = json.load(f)
    if type(cfg) != list:
        raise Exception('language config must be a list')
    for lang in cfg:
        if type(lang) != dict:
            raise Exception('each language in language config must be a dict')
        if 'language' not in lang:
            raise Exception('need "language" item for name of language')
        if 'pattern' not in lang:
            raise Exception('"pattern" is necessary for "%s", which must be a regular expression to match filenames' % lang["language"])
        if 'execute' not in lang:
            raise Exception('"execute" is necessary for "%s", which must a list of args' % lang["language"])
        if '%(arg)s' not in lang['execute']:
            raise Exception('"%(arg)s" is necessary for "execute" in "%s", which stands for the arg of testa protocol' % lang["language"])
    return cfg

kOk = 'OK'
kError = 'Error'
kTimeout = 'Timeout'
kCancel = 'Cancel'

gCancelled = False

def work(opts, qin, qout):
    global gCancelled
    try:
        while True:
            cs = qin.get()
            if cs == None:
                break
            if gCancelled:
                break
            args = shlex.split(cs['execute'])
            kws = {}
            with open(cs['stdout'], 'wb') as stdout, open(cs['stderr'], 'wb') as stderr:
                kws['stdout'] = stdout
                kws['stderr'] = stderr
                kws['cwd'] = cs['cwd']
                cs['start'] = datetime.utcnow()
                try:
                    subprocess.check_call(args, **kws)
                    cs['stop'] = datetime.utcnow()
                    qout.put([kOk, cs['name'], cs])
                except subprocess.CalledProcessError:
                    stderr.write(str(args))
                    stderr.write('\n')
                    stderr.write(str(kws))
                    stderr.write('\n')
                    cs['stop'] = datetime.utcnow()
                    qout.put([kError, cs['name'], cs])
    except KeyboardInterrupt:
        qout.put([kCancel, 'Ctrl-C'])
    except Exception as ex:
        qout.put([kCancel, str(ex)])

def launchWorkers(opts):
    reqQ = Queue()
    resQ = Queue()
    workers = [threading.Thread(target=work, args=(opts, reqQ, resQ)) for _ in range(opts.jobs)]
    for w in workers:
        w.start()
    return reqQ, resQ, workers

def stopWorkers(reqQ, workers):
    for _ in range(len(workers)):
        reqQ.put(None)
    for w in workers:
        w.join()

def error(msg):
    print msg
    sys.exit(1)

def findMatchLanguage(exe, langs):
    for lang in langs:
        if re.match(lang['pattern'], exe):
            return lang
    return {'language': None, 'execute': '%(prog)s %(arg)s'}

def getExecutableArgs(exe, langs):
    lang = findMatchLanguage(exe, langs)
    exe = op.abspath(exe)
    return lang['execute'] % {'prog': exe, 'arg': '--show-cases'}

def collectCases(opts, langs, reqQ, resQ):
    exes = []
    for exe in opts.executables:
        exeArgs = getExecutableArgs(exe, langs)
        progDir = op.abspath(op.dirname(exe))
        testDir = op.abspath(op.join(opts.dir, exe))
        if not op.exists(testDir):
            os.makedirs(testDir)
        exes.append({
            'name': exe,
            'execute': exeArgs,
            'cwd': progDir,
            'stdout': op.join(testDir, 'cases.out'),
            'stderr': op.join(testDir, 'cases.err')})
    for e in exes:
        reqQ.put(e)

    cases = []
    for _ in range(len(exes)):
        res = resQ.get()
        assert res[0] == kOk, res
        exe = res[1]
        with open(res[2]['stdout']) as f:
            cs = [x.strip() for x in f]
        cs = [x for x in cs if x]
        lang = findMatchLanguage(exe, langs)
        for c in cs:
            cases.append({
                'name': '%s/%s' % (exe, c),
                'execute': lang['execute'] % {'prog': op.abspath(exe), 'arg': c},
                'cwd': res[2]['cwd'],
                'stdout': op.join(opts.dir, exe, '%s.out' % c),
                'stderr': op.join(opts.dir, exe, '%s.err' % c)})
    return cases

def colored(s, color):
    if not os.isatty(sys.stdout.fileno()):
        return s
    else:
        return termcolor.colored(s, color)

def filterCases(opts, cases):
    cases = [x for x in cases if not re.search(opts.exclude, x['name'])]
    cases = [x for x in cases if re.search(opts.include, x['name'])]
    return cases

def dispatchCases(cases, reqQ):
    for cs in cases:
        reqQ.put(cs)

def collectResults(opts, cases, resQ):
    passed = []
    failed = []
    caseNum = len(cases)
    while len(passed) + len(failed) < caseNum:
        res = resQ.get()
        if res[0] == kOk:
            passed.append(res[2])
            result = colored('pass', 'green')
        elif res[0] == kError:
            failed.append(res[2])
            result = colored('fail', 'red')
        elif res[0] == kTimeout:
            failed.append(res[2])
            result = colored('kill', 'red')
        else:
            error(res[1])
        print('%d/%d %s: %s costs %s secs' % (
            len(passed) + len(failed), caseNum,
            result,
            res[1],
            str(res[2]['stop'] - res[2]['start'])))
    return passed, failed

if __name__ == '__main__':
    opts = parseArgs()
    langs = readLangCfg(opts.lang)

    reqQ, resQ, workers = launchWorkers(opts)
    try:
        cases = collectCases(opts, langs, reqQ, resQ)
        cases = filterCases(opts, cases)
        dispatchCases(cases, reqQ)
        _, failed = collectResults(opts, cases, resQ)
        print
        print '%d failed' % len(failed)
        for cs in failed:
            print cs['name']
    finally:
        stopWorkers(reqQ, workers)
