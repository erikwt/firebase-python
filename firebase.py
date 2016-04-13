# adapted from firebase/EventSource-Examples/python/chat.py by Shariq Hashme

from sseclient import SSEClient
import requests

from Queue import Queue
import json
import threading
import socket
import re
from firebase_token_generator import create_token


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


class RemoteThread(threading.Thread):

    def __init__(self, parent, URL, function, me, other):
        self.function = function
        self.URL = URL
        self.parent = parent
        self.me = me
        self.other = other
        super(RemoteThread, self).__init__()

    def run(self):
        try:
            self.sse = ClosableSSEClient(self.URL)
            for msg in self.sse:
                msg_data = json.loads(msg.data)
                if msg_data is None:    # keep-alives
                    continue
                msg_event = msg.event
                # TODO: update parent cache here
                self.function((msg.event, msg_data, self.me, self.other))
        except socket.error:
            pass    # this can happen when we close the stream
        except KeyboardInterrupt:
            self.close()

    def close(self):
        if self.sse:
            self.sse.close()


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


def get(URL):
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
    def __init__(self, fire_base_url, fire_base_secret):
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