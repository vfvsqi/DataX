#! /usr/bin/env python
# vim: set expandtab tabstop=4 shiftwidth=4 foldmethod=marker nu:

import sys
import ConfigParser
import json
import subprocess
import os


ldb_type = 'type'
ldb_pass = 'password'
ldb_user = 'username'
ldb_url = 'url'
ldb_table = 'tableName'
ldb_path = 'path'
ldb_file = 'fileName'


def writejson(saveFileName, readerjson, writerjson):
    dict = {}
    job = {}
    speed = {}
    speed['channel'] = 1
    errorLimit = {}
    errorLimit['record'] = 0
    errorLimit['percentage'] = 0.02
    setting = {}
    setting['speed'] = speed
    setting['errorLimit'] = errorLimit


    contents = []
    content = {}
    content['reader'] = readerjson
    content['writer'] = writerjson
    contents.append(content)

    job['content'] = contents
    job['setting'] = setting
    dict['job'] = job

    with open(saveFileName, 'w') as json_file:
        json.dump(dict, json_file)


def getFileWriterJson(indict):
    type = indict[ldb_type]
    path = indict[ldb_path]
    fileName = indict[ldb_file]
    dict = {}
    dict['name'] = type + 'writer'
    parameter = {}
    parameter['dateFormat'] = 'yyyy-MM-dd'
    parameter['writeMode'] = 'truncate'
    parameter['fileName'] = fileName
    parameter['path'] = path
    dict['parameter'] = parameter
    print(dict)
    return dict


def getFileReaderJson(indict):
    type = indict[ldb_type]
    path = indict[ldb_path]
    dict = {}
    dict['name'] = type + 'reader'
    parameter = {}
    parameter['column'] = []
    parameter['column'].append("*")
    parameter['fieldDelimiter'] = ','
    parameter['path'] = path
    dict['parameter'] = parameter
    print(dict)
    return dict


def getDbWriterJson(indict):
    type = indict[ldb_type]
    url = indict[ldb_url]
    user = indict[ldb_user]
    password = indict[ldb_pass]
    tablename = indict[ldb_table]
    dict = {}
    dict['name'] = type + 'writer'
    parameter = {}
    parameter['username'] = user
    parameter['password'] = password
    parameter['column'] = []
    parameter['column'].append("*")
    parameter['writeMode'] = 'insert'

    table = []
    table.append(tablename)

    content = {}
    content['table'] = table
    content['jdbcUrl'] = url

    contention = []
    contention.append(content)
    parameter['connection'] = contention

    dict['parameter'] = parameter
    print(dict)
    return dict


def getDbReaderJson(indict):
    type = indict[ldb_type]
    url = indict[ldb_url]
    user = indict[ldb_user]
    password = indict[ldb_pass]
    tablename = indict[ldb_table]
    dict = {}
    dict['name'] = type + 'reader'
    parameter = {}
    parameter['username'] = user
    parameter['password'] = password
    parameter['column'] = []
    parameter['column'].append("*")

    table = []
    table.append(tablename)
    jdbcurl = []
    jdbcurl.append(url)
    content = {}
    content['table'] = table
    content['jdbcUrl'] = jdbcurl

    contention = []
    contention.append(content)
    parameter['connection'] = contention

    dict['parameter'] = parameter
    print(dict)
    return dict


def createTable():
    DATAX_HOME = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    CLASS_PATH = ("%s/lib/plugin-linkoopdb-util-0.0.1-SNAPSHOT.jar") % (DATAX_HOME)
    mainName = "com.alibaba.datax.plugin.linkoopdb.Application"
    # command = "java -cp " + CLASS_PATH + " com.alibaba.datax.plugin.linkoopdb.Application " +  " ".join(args)
    list = []
    list.append("java")
    list.append("-cp")
    list.append(CLASS_PATH)
    list.append(mainName)
    print CLASS_PATH
    return list


def readerprop(prop):
    cp = ConfigParser.SafeConfigParser()
    cp.read(prop)

    readertype = cp.get('reader', 'type').lower()
    writertype = cp.get('writer', 'type').lower()
    dbs = ['linkoopdb', 'mysql', 'postgresql', 'oracle']
    files = ['txtfile', 'excel']
    readerTypeStr = 'reader'
    writerTypeStr = 'writer'
    readerdict = {}
    writerdict = {}
    if readertype in dbs:
        url = cp.get(readerTypeStr, 'url')
        user = cp.get(readerTypeStr, 'username')
        password = cp.get(readerTypeStr, 'password')
        tablename = cp.get(readerTypeStr, 'table')
        readerdict[ldb_type] = readertype
        readerdict[ldb_url] = url
        readerdict[ldb_user] = user
        readerdict[ldb_pass] = password
        readerdict[ldb_table] = tablename

        # readerdict = getDbReaderJson(readertype, url, user, password, tablename)
    elif readertype in files:
        path = cp.get(readerTypeStr, 'path')
        readerdict[ldb_type] = readertype
        readerdict[ldb_path] = path
        # readerdict = getFileReaderJson(readertype, path)
    else:
        print('error ! reader type error')

    if writertype in dbs:
        url = cp.get(writerTypeStr, 'url')
        user = cp.get(writerTypeStr, 'username')
        password = cp.get(writerTypeStr, 'password')
        tablename = cp.get(writerTypeStr, 'table')
        writerdict[ldb_type] = writertype
        writerdict[ldb_url] = url
        writerdict[ldb_user] = user
        writerdict[ldb_pass] = password
        writerdict[ldb_table] = tablename
        # writerdict = getDbWriterJson(writertype, url, user, password, tablename)
    elif writertype in files:
        path = cp.get(writerTypeStr, 'path')
        fileName = cp.get(writerTypeStr, 'fileName')
        writerdict[ldb_type] = writertype
        writerdict[ldb_path] = path
        writerdict[ldb_file] = fileName
        # writerdict = getFileWriterJson(writertype, path, fileName)
    else:
        print('error ! writer type error')

    return readerdict, writerdict


def getArgs(readerdict, writerdict):
    list = []
    list.append(readerdict[ldb_url])
    list.append(readerdict[ldb_user])
    list.append(readerdict[ldb_pass])
    list.append(readerdict[ldb_table])
    list.append(writerdict[ldb_url])
    list.append(writerdict[ldb_user])
    list.append(writerdict[ldb_pass])
    list.append(writerdict[ldb_type])
    list.append(writerdict[ldb_table])
    return list


def anaTableList(readerdict, writerdict):
    readerType = readerdict[ldb_type]
    writeType = writerdict[ldb_type]
    files = ['txtfile', 'excel', 'linkoopdb']
    if readerType == "linkoopdb" and writeType not in files:
        args = getArgs(readerdict, writerdict)
        command = createTable() + args
        print command
        p = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, err = p.communicate(b"input data that is passed to subprocess' stdin")
        if err.strip('\r\n ') != "":
            print err
            return ""
        tables = output.split("\n")
        return tables
    else:
        return readerType + "," + writeType


def getJson(readerdict, writerdict, tables):
    tmp = tables.split(',')
    if len(tmp) != 2:
        return ""
    print tables
    readerName = tmp[0]
    writeName = tmp[1]
    dbs = ['linkoopdb', 'mysql', 'postgresql', 'oracle']
    files = ['txtfile', 'excel']
    readertype = readerdict[ldb_type]
    writertype = writerdict[ldb_type]
    readerjson = {}
    writerjson = {}
    saveFileName = readertype

    otherfile = ['txtfile', 'excel', 'linkoopdb']
    if readertype == "linkoopdb" and writertype not in otherfile:
        readerdict[ldb_table] = readerName
        writerdict[ldb_table] = writeName
        saveFileName += '_' + readerName
        saveFileName += '_' + writeName

    if readertype in dbs:
        readerjson = getDbReaderJson(readerdict)
    elif readertype in files:
        readerjson = getFileReaderJson(readerdict)
    else:
        print('error ! reader type error')

    saveFileName += '_To_' + writertype

    if writertype in dbs:
        writerjson = getDbWriterJson(writerdict)
    elif writertype in files:
        writerjson = getFileWriterJson(writerdict)
    else:
        print('error ! writer type error')

    saveFileName += '.json'
    writejson(saveFileName, readerjson, writerjson)
    return saveFileName


def main(args):
    filepath = args[1]
    # filepath = '../conf/db2file.conf'
    (readerdict, writerdict) = readerprop(filepath)
    list = anaTableList(readerdict, writerdict)
    for tables in list:
        fileretnames = getJson(readerdict, writerdict, tables)
        if fileretnames != "":
            command = "python datax.py " + fileretnames
            subprocess.call(command, shell=True)


if __name__ == '__main__':
    main(sys.argv)
