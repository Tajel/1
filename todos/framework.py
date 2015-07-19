# -*- coding: utf-8 -*-
import json
import logging

from werkzeug.routing import Map, Rule
from werkzeug.wrappers import BaseRequest, BaseResponse
from werkzeug.exceptions import (HTTPException, MethodNotAllowed,
                                 NotImplemented, NotFound)

from todos.fanout import PubSubFanout
from todos.utils import parse_pgurl

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Request(BaseRequest):
    """Encapsulates a request."""


class Response(BaseResponse):
    """Encapsulates a response."""


class JSONResponse(Response):
    def __init__(self, data, *args, **kwargs):
        kwargs['content_type'] = 'application/json'
        return super(JSONResponse, self).__init__(json.dumps(data), *args, **kwargs)


class View(object):
    """Baseclass for our views."""

    allowed_methods = ('GET', 'HEAD', 'POST', 'DELETE', 'PUT')

    def __init__(self, app, req, params):
        self.app = app
        self.request = req
        self.params = params

    def get(self):
        raise MethodNotAllowed()
    post = delete = put = websocket = get

    def head(self):
        return self.get()

    def cleanup(self):
        pass

    def __call__(self, environ, start_response):
        if self.request.method not in self.allowed_methods:
            raise NotImplemented()

        if self.request.method == 'GET' and 'wsgi.websocket' in environ:
            self.ws = environ['wsgi.websocket']
            self.ws.add_close_callback(self.cleanup)

            handler = self.websocket
        else:
            handler = getattr(self, self.request.method.lower())

        resp = handler(**self.params)
        return resp(environ, start_response)


class App(object):
    def __init__(self, urls, settings):
        self.urls = urls
        self.settings = settings
        self.map, self.handlers = build_rules(urls)

    def ensure_fanout(self):
        if not hasattr(self, 'fanout'):
            self.fanout = PubSubFanout(['todos_updates'], **parse_pgurl(self.settings.db_url))


    def __call__(self, environ, start_response):
        # TODO: figure out why doing this in __init__ instead of here results in
        # corrupt/double connection messages to postgres.
        self.ensure_fanout()
        try:
            req = Request(environ)
            try:
                # convenient access to Beaker session.  Will raise KeyError if
                # Beaker middleware is not enabled.
                req.session = environ['beaker.session']
                adapter = self.map.bind_to_environ(environ)
                view_name, params = adapter.match()
                view_cls = self.handlers[view_name]
                wsgi_app = view_cls(self, req, params)
            except HTTPException, e:
                wsgi_app = e
            resp = wsgi_app(environ, start_response)

            return resp
        finally:
            if 'wsgi_app' in locals() and hasattr(wsgi_app, 'dbconn'):
                logger.debug("Closing PG connection")
                wsgi_app.dbconn.close()


def build_rules(rules_tuples):
    """
    Given a list of tuples like this:

    [
        ('/', 'index', views.Index),
        ('/hello/<name>/', 'hello', views.Hello),
    ]

    Return two things:
        1. A Werkzeug Map object.
        2. A dictionary mapping the names of the Werkzeug endpoints to view
        classes.
    """
    handlers = {}
    rules = []
    for pat, name, view in rules_tuples:
        rules.append(Rule(pat, endpoint=name))
        handlers[name] = view
    return Map(rules), handlers


def reverse(rule_map, endpoint, values=None):
    """ Given a rule map, and the name of one of our endpoints, and a dict of
    parameter values, return a URL"""
    adapter = rule_map.bind('')
    return adapter.build(endpoint, values=values)
