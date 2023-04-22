import requests

from typing import Dict

class ApiConnect:
    def __init__(self,
                 headers: Dict[str, str],
                 sort_field: str,
                 sort_direction: str,
                 page: str
                 ) -> None:

        self.headers = headers
        self.sort_field = sort_field
        self.sort_direction = sort_direction
        self.page = page

    def url(self, limit, offset, from_date=None) -> str:
        if from_date == None:
            return 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/{page}?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'.format(
                limit=limit,
                offset=offset,
                sort_field=self.sort_field,
                sort_direction=self.sort_direction,
                page=self.page)
        return 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/{page}?from={from_date}&sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'.format(
            from_date=from_date,
            limit=limit,
            offset=offset,
            sort_field=self.sort_field,
            sort_direction=self.sort_direction,
            page=self.page)
        

    def client(self, limit, offset, from_date=None):
        return requests.get(self.url(limit, offset, from_date), headers=self.headers).json()
