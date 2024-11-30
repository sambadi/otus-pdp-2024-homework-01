install:  # установка зависимостей
	poetry install

run: # запуск на исполнение
	poetry run python -m homework_01

lint: # проверка кода с помощью pre-commit
	poetry run pre-commit run --all-files

test: # запуск тестов
	poetry run pytest ./tests --cov=homework_01 --cov-report term-missing