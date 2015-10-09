import requests, json
from pymldb.util import add_repr_html_to_response

class ResourceError(Exception):
    def __init__(self, r):
        try:
            message = json.dumps(r.json(), indent=2)
        except:
            message = r.content
        super(ResourceError, self).__init__(
            "'%d %s' response to '%s %s'\n\n%s" %
            (r.status_code, r.reason, r.request.method, r.request.url,message)
        )
        self.result = r

def decorate_response(fn):
    def inner(*args, **kwargs):
        raise_on_error = True
        if "raise_on_error" in kwargs:
            raise_on_error = kwargs["raise_on_error"]
            del kwargs["raise_on_error"]
        result = add_repr_html_to_response(fn(*args, **kwargs))
        if raise_on_error and result.status_code >= 400:
            raise ResourceError(result)
        return result
    return inner

class Resource(object):
    def __init__(self, uri="http://localhost"):
        if not uri.startswith("http"):
            raise Exception("URIs must start with 'http'")
        self.uri = uri.strip("/")

    def __repr__(self):
        return self.uri

    def __str__(self):
        return self.uri

    def __call__(self, frag):
        return Resource(self.uri+"/"+str(frag).strip("/"))

    def __getattr__(self, frag):
        return Resource(self.uri+"/"+str(frag).strip("/"))

    @decorate_response
    def get(self, **kwargs):
        return requests.get(self.uri,
            params= {k: (json.dumps(v) if isinstance(v, dict) else v)
                 for k,v in kwargs.items() })

    @decorate_response
    def put(self, payload):
        return requests.put(self.uri, json=payload)

    @decorate_response
    def post(self, payload):
        return requests.post(self.uri, json=payload)

    @decorate_response
    def delete(self):
        return requests.delete(self.uri)
