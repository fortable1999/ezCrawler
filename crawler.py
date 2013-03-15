import urllib2
import sys, re
import multiprocessing
import functools
import crawlconf

url_set = set([])
LOG = crawlconf.LOG
MASTER_SLAVE = crawlconf.MASTER_SLAVE

def get_childnodes(url, pattern):
    if url in url_set: return []
    if not re.match(pattern, url): return []

    if LOG:
        print "-> catch %s" % url 
    try:
        data = urllib2.urlopen(url).read()
    except urllib2.HTTPError:
        return []
    res = map(lambda s: s[6:-1],re.findall(r'href=\"\S*\"', data))
    res = filter(lambda s: re.match(pattern, s), res)
    return set(res)

def start_crawl(target_url, url_set = set(), pattern = r'\S'):
    new_url_set = set([])
    new_url_set.update([target_url])
    worker = functools.partial(get_childnodes, pattern = crawlconf.PATTERN)
    while new_url_set:
        if MASTER_SLAVE:
            pool = multiprocessing.Pool(crawlconf.WORKERS)
            catched_urls_set = pool.map(worker, [i for i in new_url_set])
        else:
            catched_urls_set = map(worker, [i for i in new_url_set])
        catched_urls_set = set(reduce(lambda x,y: x.union(y), catched_urls_set))
        url_set.update(new_url_set)
        new_url_set = url_set - new_url_set
        new_url_set = catched_urls_set - url_set
    return list(url_set)

if __name__ == "__main__":
    all_items = start_crawl(target_url = crawlconf.ROOT_URL, pattern = crawlconf.PATTERN)
    for i in all_items:
        if LOG:
            print i
    print "%d items caught." % len(all_items)
