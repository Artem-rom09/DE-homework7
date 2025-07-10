# Проєкт Аналітики Відгуків Amazon

Цей проєкт обробляє дані відгуків Amazon, завантажує їх у Cassandra та надає доступ через REST API з кешуванням у Redis.

## Інструкція по запуску

1.  **Покладіть файл з даними** у корінь проєкту та переконайтесь, що його назва `amazon_reviews.csv`.

2.  **Запустіть інфраструктуру** (Cassandra, Redis, API):
    ```bash
    docker-compose up --build -d
    ```
    Зачекайте хвилину, поки сервіси стабілізуються.

3.  **Створіть схему в Cassandra:**
    ```bash
    docker exec -i cassandra-reviews cqlsh < schema.cql
    ```

4.  **Запустіть ETL процес** для завантаження даних. Встановіть `pyspark` локально (`pip install pyspark`) та виконайте:
    ```bash
    python etl.py
    ```
   

5.  **Перевірте API.** Відкрийте браузер за адресою [http://localhost:8000/docs](http://localhost:8000/docs) для інтерактивної документації або використайте `curl`.

## Приклади запитів

```bash
# Отримати відгуки для продукту
curl -X GET "http://localhost:8000/reviews/product/0316769487"

# Отримати відгуки з рейтингом 5 для продукту
curl -X GET "http://localhost:8000/reviews/product/0316769487/rating/5"

# Отримати відгуки клієнта
curl -X GET "http://localhost:8000/reviews/customer/52942582"

# Отримати топ-5 найпопулярніших продуктів за період (напр. 2005-08)
curl -X GET "http://localhost:8000/analytics/most-reviewed?period=2005-08&n=5"

# Отримати топ-5 найпродуктивніших клієнтів
curl -X GET "http://localhost:8000/analytics/most-productive?period=2005-08&n=5"

# Отримати топ-5 "хейтерів"
curl -X GET "http://localhost:8000/analytics/haters?period=2005-08&n=5"

# Отримати топ-5 "прихильників"
curl -X GET "http://localhost:8000/analytics/backers?period=2005-08&n=5"
```
