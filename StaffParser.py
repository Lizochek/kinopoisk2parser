import requests


class StaffParser:
    BASE_URL = 'https://kinopoiskapiunofficial.tech/api'

    def __init__(self, api_key):
        """Инициализация парсера с API-ключом"""
        self.api_key = api_key

    @staticmethod
    def _make_request(url, headers, params=None):
        """Отправка GET-запроса и возврат JSON-ответа"""
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    def get_staff_by_film_id(self, film_id):
        """Получение данных о персонале по ID фильма"""
        url = f'{self.BASE_URL}/v1/staff'
        headers = {'X-API-KEY': self.api_key}
        params = {'filmId': film_id}
        response = self._make_request(url, headers, params)
        return response

    def parse_staff_data(self, staff):
        """Парсинг данных о персонале"""
        parsed = {
            'name': staff['nameRu'],
            'profession': staff['professionText'],
            # 'profession2': staff['professionKey'],
            # 'description': staff['description']
        }
        return parsed
