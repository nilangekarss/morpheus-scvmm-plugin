# conftest.py
import ssl

import pytest
import logging
import os

import requests
import urllib3
from hpe_morpheus_automation_libs.api.external_api.morpheus_session import MorpheusExternalAPISession

log = logging.getLogger(__name__)

@pytest.fixture
def morpheus_session():
    host = os.getenv("BASE_URL")
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")
    return MorpheusExternalAPISession(host=host, username=username, password=password, version="v8_0_6")

"""
Configuration for vcenter tests to handle SSL issues
"""


# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Disable SSL verification globally
ssl._create_default_https_context = ssl._create_unverified_context

# Monkey patch requests to always disable SSL verification
original_request = requests.Session.request


def no_ssl_request(self, method, url, **kwargs):
    kwargs["verify"] = False
    return original_request(self, method, url, **kwargs)


requests.Session.request = no_ssl_request

# Also patch the get method specifically
original_get = requests.get
original_post = requests.post
original_put = requests.put
original_delete = requests.delete


def no_ssl_get(*args, **kwargs):
    kwargs["verify"] = False
    return original_get(*args, **kwargs)


def no_ssl_post(*args, **kwargs):
    kwargs["verify"] = False
    return original_post(*args, **kwargs)


def no_ssl_put(*args, **kwargs):
    kwargs["verify"] = False
    return original_put(*args, **kwargs)


def no_ssl_delete(*args, **kwargs):
    kwargs["verify"] = False
    return original_delete(*args, **kwargs)


requests.get = no_ssl_get
requests.post = no_ssl_post
requests.put = no_ssl_put
requests.delete = no_ssl_delete