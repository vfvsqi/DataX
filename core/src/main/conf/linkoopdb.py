#! /usr/bin/env python
# vim: set expandtab tabstop=4 shiftwidth=4 foldmethod=marker nu:

import re
import sys
import time
import ConfigParser
import json


def writejson(filename, reader, wirter):
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
    content['reader'] = reader
    content['writer'] = wirter
    contents.append(content)

    job['content'] = contents
    job['setting'] = setting
    dict['job'] = job

    with open(filename, 'w') as json_file:
        json.dump(dict, json_file)


def getFileWriterJson(type, path, fileName):
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


def getFileReaderJson(type, path):
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


def getDbWriterJson(type, url, user, password, tablename):
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


def getDbReaderJson(type, url, user, password, tablename):
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
        readerdict = getDbReaderJson(readertype, url, user, password, tablename)
    elif readertype in files:
        path = cp.get(readerTypeStr, 'path')
        readerdict = getFileReaderJson(readertype, path)
    else:
        print('error ! reader type error')

    if writertype in dbs:
        url = cp.get(writerTypeStr, 'url')
        user = cp.get(writerTypeStr, 'username')
        password = cp.get(writerTypeStr, 'password')
        tablename = cp.get(writerTypeStr, 'table')
        writerdict = getDbWriterJson(writertype, url, user, password, tablename)
    elif writertype in files:
        path = cp.get(writerTypeStr, 'path')
        fileName = cp.get(writerTypeStr, 'fileName')
        writerdict = getFileWriterJson(writertype, path, fileName)
    else:
        print('error ! writer type error')

    saveFileName = readertype + '2' + writertype + '.json'
    writejson(saveFileName, readerdict, writerdict)
    print(saveFileName)
    return saveFileName


def main(args):
    header = '../conf/'
    conffiles = ['db2db.conf', 'db2file.conf', 'file2file.conf', 'file2db.conf']
    # conffiles = ['db2db.conf']

    for t in conffiles:
        temp = header + t
        readerprop(temp)


if __name__ == '__main__':
    main(sys.argv)
