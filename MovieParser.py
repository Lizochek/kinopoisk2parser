import requests


class MovieParser:
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

    def get_film_by_id(self, film_id):
        """Получение данных фильма по его ID"""
        url = f'{self.BASE_URL}/v2.2/films/{film_id}'
        headers = {'X-API-KEY': self.api_key}
        response = self._make_request(url, headers)
        return response

    def parse_film_data(self, film):
        """Парсинг данных фильма"""
        parsed = {
            'filmId': film['kinopoiskId'],
            'title': film['nameRu'],
            'reviewsCount': film['reviewsCount'],
            'year': film['year'],
            'country': film["countries"][0]["country"],
            'genre': film["genres"][0]["genre"],
            'rating': film['ratingKinopoisk'],
            'voteCount': film['ratingKinopoiskVoteCount'],
            'ratingAgeLimits': film['ratingAgeLimits'][3:]
        }
        return parsed
