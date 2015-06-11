#
# pymldb
# Copyright (c) 2013 Datacratic. All rights reserved.
#

import types, json
from pygments import highlight
from pygments.lexers import JsonLexer
from pygments.formatters import HtmlFormatter
from pygments.style import Style
from pygments.token import Keyword, Name, String, Number

class PygmentsStyle(Style):
    default_style = ""
    styles = {
        Name: 'bold #333', String: '#00d', Number: '#00d', Keyword: '#00d'
    }
        
def add_repr_html_to_response(resp):
    def _repr_html_(self):
        result = "<strong>%s %s</strong><br />" % (self.request.method, self.request.url)
        color = "black"
        if self.status_code >= 400: color = "red" 
        if self.status_code < 300: color = "green" 
        result += "<strong style=\"color: %s;\">%d %s</strong><br /> " % (color, self.status_code, self.reason)
        if "content-type" in self.headers:
            if self.headers["content-type"] == "application/json":
                result += highlight(
                    json.dumps(self.json(), indent=2), 
                    JsonLexer(), 
                    HtmlFormatter(noclasses = True, nobackground =True, style=PygmentsStyle)
                    )
            elif self.headers["content-type"] == "text/html":
                result += self.content
        return result
    resp._repr_html_ = types.MethodType(_repr_html_, resp)
    return resp

