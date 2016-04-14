# adapted from firebase/EventSource-Examples/python/chat.py by Shariq Hashme

from sseclient import SSEClient
import requests

from Queue import Queue
import json
import threading
import socket
import re
from firebase_token_generator import create_token
from urllib import urlencode
from collections import OrderedDict
from requests.exceptions import HTTPError


class ClosableSSEClient(SSEClient):

    def __init__(self, *args, **kwargs):
        self.should_connect = True
        super(ClosableSSEClient, self).__init__(*args, **kwargs)

    def _connect(self):
        if self.should_connect:
            super(ClosableSSEClient, self)._connect()
        else:
            raise StopIteration()

    def close(self):
        self.should_connect = False
        self.retry = 0
        try:
            self.resp.raw._fp.fp._sock.shutdown(socket.SHUT_RDWR)
            self.resp.raw._fp.fp._sock.close()
        except AttributeError:
            pass


class Stream:
    def __init__(self, url, stream_handler):
        self.url = url
        self.stream_handler = stream_handler
        self.sse = None
        self.thread = None
        self.start()

    def start(self):
        self.thread = threading.Thread(target=self.start_stream,
                                       args=(self.url, self.stream_handler))
        self.thread.start()
        return self

    def start_stream(self, url, stream_handler):
        self.sse = ClosableSSEClient(url)
        messages = []
        for msg in self.sse:
            msg_data = json.loads(msg.data)
            # don't return initial data
            if msg_data['path'] != '/':
                msg_data["event"] = msg.event
                messages.append(msg_data)
                stream_handler(messages)

    def close(self):
        self.sse.close()
        self.thread.join()
        return self



def firebaseURL(URL):
    if '.firebaseio.com' not in URL.lower():
        if '.json' == URL[-5:]:
            URL = URL[:-5]
        if '/' in URL:
            if '/' == URL[-1]:
                URL = URL[:-1]
            URL = 'https://' + \
                URL.split('/')[0] + '.firebaseio.com/' + URL.split('/', 1)[1]
        else:
            URL = 'https://' + URL + '.firebaseio.com/.json'

    if 'http://' in URL:
        URL = URL.replace('http://', 'https://')
    if 'https://' not in URL:
        URL = 'https://' + URL
    if '.json' not in URL.lower():
        if '/' != URL[-1]:
            URL = URL + '/.json'
        else:
            URL = URL + '.json'
    return URL


class subscriber:

    def __init__(self, URL, function, me=None, other=None):
        self.cache = {}
        self.remote_thread = RemoteThread(self, firebaseURL(URL), function, me, other)

    def start(self):
        self.remote_thread.start()

    def stop(self):
        self.remote_thread.close()
        self.remote_thread.join()

    def wait(self):
        self.remote_thread.join()


class FirebaseException(Exception):
    pass


def put(URL, msg):
    to_post = json.dumps(msg)
    response = requests.put(firebaseURL(URL), data=to_post)
    if response.status_code != 200:
        raise FirebaseException(response.text)


def patch(URL, msg):
    to_post = json.dumps(msg)
    response = requests.patch(firebaseURL(URL), data=to_post)
    if response.status_code != 200:
        raise FirebaseException(response.text)


def get(URL, token=None):
    response = requests.get(firebaseURL(URL))
    if response.status_code != 200:
        raise FirebaseException(response.text)
    return json.loads(response.text)


def post(URL, msg):
    to_post = json.dumps(msg)
    response = requests.post(firebaseURL(URL), data=to_post)
    if response.status_code != 200:
        raise Exception(response.text)
    return json.loads(response.text)


def delete(URL):
    response = requests.delete(firebaseURL(URL))
    if response.status_code != 200:
        raise FirebaseException(response.text)


class FirebaseAuthentication():
    """ Firebase Interface """
    def __init__(self, fire_base_url, fire_base_secret, authentication=None):
        if not fire_base_url.endswith('/'):
            url = ''.join([fire_base_url, '/'])
        else:
            url = fire_base_url
        # find db name between http:// and .firebaseio.com
        db_name = re.search('https://(.*).firebaseio.com', fire_base_url)
        if db_name:
            name = db_name.group(1)
        else:
            db_name = re.search('(.*).firebaseio.com', fire_base_url)
            name = db_name.group(1)
        auth_payload = {"uid": "1"}
        options = {"admin": True}

        self.token = create_token(fire_base_secret, auth_payload, options)
        self.requests = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=3)
        for scheme in ('http://', 'https://'):
            self.requests.mount(scheme, adapter)
        self.fire_base_url = url
        self.fire_base_name = name
        self.secret = fire_base_secret
        self.path = ""
        self.build_query = {}
        self.last_push_time = 0
        self.last_rand_chars = []
        self.authentication = authentication

    def authenticate(self, email, password):
        request_ref = 'https://auth.firebase.com/auth/firebase?firebase={0}&email={1}&password={2}'.\
            format(self.fire_base_name, email, password)
        request_object = self.requests.get(request_ref)
        return request_object.json()


    def create(self, email, password):
        request_ref = 'https://auth.firebase.com/auth/firebase/create?firebase={0}&email={1}&password={2}'.\
            format(self.fire_base_name, email, password)
        request_object = self.requests.get(request_ref)
        request_object.raise_for_status()
        return request_object.json()

    def child(self, *args):
        new_path = "/".join(args)
        if self.path:
            self.path += "/{}".format(new_path)
        else:
            if new_path.startswith("/"):
                new_path = new_path[1:]
            self.path = new_path

        return self

    def get(self, token=None):
        build_query = self.build_query
        query_key = self.path.split("/")[-1]
        request_ref = self.build_request_url(token)
        # do request
        request_object = self.requests.get(request_ref)
        try:
            request_object.raise_for_status()
        except HTTPError as e:
            # raise detailed error message
            raise HTTPError(e, request_object.text)

        request_dict = request_object.json()
        # if primitive or simple query return
        if not isinstance(request_dict, dict):
            return PyreResponse(request_dict, query_key)
        if not build_query:
            return PyreResponse(convert_to_pyre(request_dict.items()), query_key)
        # return keys if shallow
        if build_query.get("shallow"):
            return PyreResponse(request_dict.keys(), query_key)
        # otherwise sort
        sorted_response = None
        if build_query.get("orderBy"):
            if build_query["orderBy"] == "$key":
                sorted_response = sorted(request_dict.items(), key=lambda item: item[0])
            else:
                sorted_response = sorted(request_dict.items(), key=lambda item: item[1][build_query["orderBy"]])
        return PyreResponse(convert_to_pyre(sorted_response), query_key)

    def push(self, data, token=None):
        request_token = check_token(token, self.token)
        request_ref = '{0}{1}.json?auth={2}'.format(self.fire_base_url, self.path, request_token)
        self.path = ""
        request_object = self.requests.post(request_ref, data=json.dumps(data))
        return request_object.json()

    def set(self, data, token=None):
        request_token = check_token(token, self.token)
        request_ref = '{0}{1}.json?auth={2}'.format(self.fire_base_url, self.path, request_token)
        self.path = ""
        request_object = self.requests.put(request_ref, data=json.dumps(data))
        return request_object.json()

    def update(self, data, token=None):
        request_token = check_token(token, self.token)
        request_ref = '{0}{1}.json?auth={2}'.format(self.fire_base_url, self.path, request_token)
        self.path = ""
        request_object = self.requests.patch(request_ref, data=json.dumps(data))
        return request_object.json()

    def build_request_url(self, token):
        parameters = {}
        parameters['auth'] = check_token(token, self.token)
        for param in list(self.build_query):
            if type(self.build_query[param]) is str:
                parameters[param] = quote('"' + self.build_query[param] + '"')
            else:
                parameters[param] = self.build_query[param]
        # reset path and build_query for next query
        request_ref = '{0}{1}.json?{2}'.format(self.fire_base_url, self.path, urlencode(parameters))
        self.path = ""
        self.build_query = {}
        return request_ref

    def stream(self, stream_handler, token=None):
        request_ref = self.build_request_url(token)
        return Stream(request_ref, stream_handler)

def convert_to_pyre(items):
    pyre_list = []
    for item in items:
        pyre_list.append(Pyre(item))
    return pyre_list

class PyreResponse:
    def __init__(self, pyres, query_key):
        self.pyres = pyres
        self.query_key = query_key

    def val(self):
        if isinstance(self.pyres, list):
            # unpack pyres into OrderedDict
            pyre_list = []
            for pyre in self.pyres:
                pyre_list.append((pyre.key(), pyre.val()))
            return OrderedDict(pyre_list)
        else:
            # return primitive or simple query results
            return self.pyres

    def key(self):
        return self.query_key

    def each(self):
        if isinstance(self.pyres, list):
            return self.pyres


class Pyre:
    def __init__(self, item):
        self.item = item

    def val(self):
        return self.item[1]

    def key(self):
        return self.item[0]

def check_token(user_token, admin_token):
    if user_token:
        return user_token
    else:
        return admin_token

