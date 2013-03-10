import urllib2 as urllib
import sys, re, itertools
from itertools import repeat
import multiprocessing

url_set = set([])
LOG = True
MASTER_SLAVE = True

def update_urllist(url_set, urls):
    url_set = url_set.union(urls)

def get_childnodes(url, pattern = r'http://\S*yahoo.com'):
    if url in url_set: return []
    if not re.match(pattern, url): return []

    if LOG:
        print "-> catch %s" % url 
    try:
        data = urllib.urlopen(url).read()
    except urllib.HTTPError:
        return []
    res = map(lambda s: s[6:-1],re.findall(r'href=\"\S*\"', data))
    res = filter(lambda s: re.match(pattern, s), res)
    res = filter(lambda x: x != '[#/]', res)
    return set(res)

def start_crawl(target_url, url_set = set(), pattern = r'\S'):
    new_url_set = set([])
    new_url_set.update([target_url])
    while new_url_set:
        if MASTER_SLAVE:
            pool = multiprocessing.Pool(40)
            catched_urls_set = pool.map(get_childnodes, [i for i in new_url_set])
        else:
            catched_urls_set = map(get_childnodes, [i for i in new_url_set])
        catched_urls_set = set(reduce(lambda x,y: x.union(y), catched_urls_set))
        url_set.update(new_url_set)
        new_url_set = url_set - new_url_set
        new_url_set = catched_urls_set - url_set
    return list(url_set)

if __name__ == "__main__":
    all_items = start_crawl(target_url = sys.argv[1], pattern = r'http://\S*yahoo.com')
    for i in all_items:
        if LOG:
            print i
    print "%d items caught." % len(all_items)
