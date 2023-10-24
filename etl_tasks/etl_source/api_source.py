import requests
import json


class API_SOURCE():

    def __init__(self, def_url):
        self.url_for_requests = def_url

    def call_api (self, def_method):
        answer = requests.request(
            method = def_method
            , url = self.url_for_requests
            )
        list_dictData= json.loads(answer.text)
        return (list_dictData)
