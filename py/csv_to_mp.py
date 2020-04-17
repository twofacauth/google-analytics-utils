#!/usr/bin/env python3
import sys
import csv
import requests

'''
Usage:
  python csv_to_mp.py src.csv

src.csv
The first line is param names of measurement protocol like below.

cd1,cd2,cd3
aaa,1,value1
bbb,0,value2
'''

def getEncode(filepath):
  encs = "iso-2022-jp euc-jp cp932 utf-8".split()
  for enc in encs:
    with open(filepath, encoding=enc) as fr:
      try:
        fr = fr.read()
      except UnicodeDecodeError:
        continue
    return enc

if len(sys.argv) != 2:
  print('Usage: python %s filename' % sys.argv[0])
  quit()

strUa = 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:46.0) Gecko/20100101 Firefox/46.0'
strUrl = 'https://www.google-analytics.com/collect'
#strUrl = 'https://www.google-analytics.com/debug/collect' # debug

dParamsCommon = {
  'v': 1,
  'tid': 'UA-99999999-1',
  't': 'event',
  'ec': 'mp_import',
  'ea': 'crm',
  'ni': 1
}

with open(sys.argv[1], mode = 'r', encoding=getEncode(sys.argv[1])) as f1:
  reader = csv.DictReader(f1, delimiter=',')
  i = 1
  for row in reader:
    row.update(dParamsCommon)
    try:
      r = requests.post(strUrl, data = row, headers = {
        'User-Agent': strUa # When user-agent is not specified , the hit is not counted.
      })
      r.raise_for_status()
      print('Line {}: {}'.format(i, r.status_code))
    except Exception as e:
      print('Line {}: {}'.format(i, e))
    finally:
      i += 1
