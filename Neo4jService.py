from neo4j import GraphDatabase


class Neo4jService:
    def __init__(self, uri, user, password):
        """Инициализация сервиса с указанными параметрами подключения"""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        """Закрытие подключения к базе данных"""
        self.driver.close()

    def add_person(self, name, profession):
        """Добавление нового персонажа с заданным именем и профессией"""
        with self.driver.session() as session:
            session.write_transaction(self._create_and_return_person, name, profession)

    @staticmethod
    def _create_and_return_person(tx, name, profession):
        """Создание и возврат нового персонажа"""
        query = (
            "CREATE (p:Person {name: $name, profession: $profession}) "
            "RETURN p"
        )
        result = tx.run(query, name=name, profession=profession)
        return result.single()[0]

    def add_movie(self, title):
        """Добавление нового фильма с заданным названием"""
        with self.driver.session() as session:
            session.write_transaction(self._create_and_return_movie, title)

    @staticmethod
    def _create_and_return_movie(tx, title):
        """Создание и возврат нового фильма"""
        query = (
            "CREATE (m:Movie {title: $title}) "
            "RETURN m"
        )
        result = tx.run(query, title=title)
        return result.single()[0]

    def add_relationship(self, person_name, movie_title, relation, description):
        """Добавление нового отношения между персонажем и фильмом"""
        with self.driver.session() as session:
            session.write_transaction(self._create_and_return_relationship, person_name, movie_title, relation,
                                      description)

    @staticmethod
    def _create_and_return_relationship(tx, person_name, movie_title, relation, description):
        """Создание и возврат нового отношения"""
        query = (
            "MATCH (p:Person {name: $person_name}), (m:Movie {title: $movie_title}) "
            f"CREATE (p)-[r:{relation} {{roles: $description}}]->(m) "
            "RETURN r"
        )
        result = tx.run(query, person_name=person_name, movie_title=movie_title, description=description)
        return result.single()[0]
