import urllib
import datetime
import pytz
from jinja2 import Template, Environment, PackageLoader



######################
##
#   TEMPLATE FILTERS
##
#####
def date(value, fmt='%d %b %Y'):
    return value.strftime(fmt)

def age(date, units='days'):
    return getattr((datetime.datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(date.tzinfo) - date), units, None)

def humansize(num):
    for x in ['bytes','KB','MB','GB']:
        if num < 1024.0:
            return "%3.1f%s" % (num, x)
        num /= 1024.0
    return "%3.1f%s" % (num, 'TB')

def query(request, **kwargs):
    base = request.GET
    base.update(kwargs)
    return urllib.urlencode(base)

def keys(data):
    return data.keys()

def url(name, *args, **kwargs):
    from pynzbdex.web.router import router
    return router.url_reverse(str(name), *args, **kwargs)

templates = Environment(loader=PackageLoader('pynzbdex.web', 'templates'),
                        autoescape=True)
templates.filters.update({
        'date': date,
        'age': age,
        'humansize': humansize,
        'query': query,
        'keys': keys,
        'url': url,
    })
