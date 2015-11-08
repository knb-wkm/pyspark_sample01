# -*- coding: utf-8 -*-
from xml.dom import minidom
import sys

"""
wikipediaのダンプをtsv形式で出力するスクリプト
"""
def get_title_text(dom):
    title = dom.getElementsByTagName("title").item(0).childNodes[0].data
    text  = dom.getElementsByTagName("text").item(0).childNodes[0].data
    return {"title": title, "text": text}

def has_title_text(dom):
    try:
        get_title_text(dom)
        return True
    except:
        return False

if len(sys.argv) != 2:
    print "[error] ex. $ python jawiki_parser.py data/jawiki-latest-pages-articles.xml"
    quit()

xdoc = minidom.parse(sys.argv[1])
node = xdoc.getElementsByTagName("page")

title_texts = [get_title_text(pages) for pages in node if has_title_text]
for title_text in title_texts:
    print "\"%s\"\t\"%s\"" % (title_text["title"].encode('utf-8'), title_text["text"].encode('utf-8'))
