
import json
import pandas as pd
import requests

class airTable:
    def __init__(self, apiKey, baseId, apiVersion='v0', endpoint=None):
        self.apiKey = apiKey
        self.apiVersion = apiVersion
        self.baseId = baseId
        self.endpoint = endpoint

    def _fetch_records(self, url, headers):
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise ValueError(f"Error: {response.status_code} - {response.reason}")
        payload = response.json()
        offset = None
        if 'offset' in payload.keys():
            offset = f"?offset={payload['offset']}"
        return response, offset

    def _transform_dataframe4sink(self, data):
        fields = data.fillna('').to_dict('records')
        fields = [{k: v for k, v in d.items() if v and v.strip()} for d in fields]

        dataframe = pd.DataFrame(data={'fields': fields})
        records = dict({'records': dataframe.to_dict(orient='records')})
        sink = json.dumps(records)

        return sink

    def query_table(self, endpoint):
        url = f"https://api.airtable.com/{self.apiVersion}/{self.baseId}/{endpoint}"
        headers = {
            'Authorization': f'Bearer {self.apiKey}',
            'Content-Type': 'application/json'
        }

        records = list()
        offset = ""
        while (True):
            response, offset = self._fetch_records(url + offset, headers=headers)
            records += response.json()['records']
            if not (offset):
                break
        dataframe = pd.json_normalize(records)
        return dataframe

    def insert_records(self, endpoint, dataframe, pagination_max=10):
        url = f"https://api.airtable.com/{self.apiVersion}/{self.baseId}/{endpoint}"
        headers = {
            'Authorization': f'Bearer {self.apiKey}',
            'Content-Type': 'application/json',
        }
        log = list()
        for i in range(0, dataframe.shape[0], pagination_max):
            payload = dataframe[i:(i + pagination_max)].copy()
            data = self._transform_dataframe4sink(data=payload)
            response = requests.post(url, headers=headers, data=data)
            if response.status_code != 200:
                raise ValueError(f"Error: {response.status_code} - {response.reason}")
            log += response.json()['records']
        logs = {'created_records': log}
        return logs

    def delete_records(self, endpoint, records, pagination_max=10):
        import requests

        url = f"https://api.airtable.com/{self.apiVersion}/{self.baseId}/{endpoint}"
        headers = {
            'Authorization': f'Bearer {self.apiKey}',
            'Content-Type': 'application/json',
        }
        log = list()
        for i in range(0, len(records), pagination_max):
            payload = {'records[]': records[i:(i + pagination_max)]}
            response = requests.delete(url, headers=headers, params=payload)
            if response.status_code != 200:
                raise ValueError(f"Error: {response.status_code} - {response.reason}")
            log += response.json()['records']
        return {'records': log}
