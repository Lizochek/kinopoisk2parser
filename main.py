import pandas as pd
from MovieParser import MovieParser
from StaffParser import StaffParser
from Neo4jService import Neo4jService
import requests
import os

# API-ключ для Кинопоиска
API_KEY = '9aa82908-75d6-49a4-b8d5-7d1c82c592ec'

# Создаем объекты для работы с Neo4j и парсинга данных
neo4j = Neo4jService("neo4j+s://fe436181.databases.neo4j.io", "neo4j", "OdAJh2sqkZNTfyzYKo2bmgmf3RMv-mc4Dbyr53f9WB4")
movie_parser = MovieParser(API_KEY)
staff_parser = StaffParser(API_KEY)

# создал директорию для хранения файлов csv
os.makedirs('data/', exist_ok=True)

films_df = pd.DataFrame()
persons_df = pd.DataFrame()
# Обрабатываем фильмы с id от 356 до 500
for film_id in range(311, 313):
    try:
        # Получаем и обрабатываем данные о фильме
        film = movie_parser.get_film_by_id(film_id)
        parsed_film = movie_parser.parse_film_data(film)
        print(f"Film: {parsed_film}")

        # добавляем инфу о фильме
        films_df = pd.concat([films_df,pd.DataFrame(parsed_film, index=[0])], ignore_index=True)

        # Добавляем фильм в Neo4j
        #neo4j.add_movie(parsed_film['title'])
        # Получаем и обрабатываем данные о съемочной группе
        staff = staff_parser.get_staff_by_film_id(parsed_film['filmId'])
        for person in staff:
            parsed_person = staff_parser.parse_staff_data(person)
            print(f"Staff: {parsed_person}")

            # добавляем информацию о человеке (ключ - айдишник фильма)
            persons_df = pd.concat([persons_df, pd.DataFrame({'film_id': parsed_film['filmId'], **parsed_person}, index=[0])],
                                   ignore_index=True)

            # Добавляем члена съемочной группы в Neo4j
            #neo4j.add_person(parsed_person['name'], parsed_person['profession'])

            # Добавляем связь между членом съемочной группы и фильмом в Neo4j
            #neo4j.add_relationship(parsed_person['name'], parsed_film['title'], parsed_person['profession2'], parsed_person['description'])

        print("------")
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except Exception as err:
        print(f"An error occurred: {err}")

# Закрываем соединение с Neo4j
neo4j.close()

films_df.to_csv('data/films.csv', index=0)
persons_df.to_csv('data/persons.csv', index=0)
