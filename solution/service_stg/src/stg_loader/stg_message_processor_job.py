import json
import sys

import time
from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from stg_loader.repository.stg_repository import StgRepository


class StgMessageProcessor:
    _consumer: KafkaConsumer = None
    _producer: KafkaProducer = None
    _redis: RedisClient = None
    _stg_repository: StgRepository = None
    _batch_size: int = 100
    _logger: Logger = None

    def __init__(
                    self,
                    kafka_consumer: KafkaConsumer,
                    kafka_producer: KafkaProducer,
                    redis_client: RedisClient,
                    stg_repository: StgRepository,
                    batch_size: int,
                    logger: Logger
                ) -> None:
        self._consumer = kafka_consumer
        self._producer = kafka_producer
        self._redis = redis_client
        self._stg_repository = stg_repository
        self._batch_size = batch_size
        self._logger = logger

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        processed_messages = 0;
        timeout: float = 3.0
        while processed_messages < self._batch_size:
            # 1. Получите сообщение из Kafka с помощью `consume`.
            dct_msg = self._consumer.consume(timeout=timeout)

            if dct_msg is None:
                # если в Kafka сообщений нет
                break
            else:
                # 2. Сохраните сообщение в таблицу, используя `_stg_repository`.
                self._stg_repository.order_events_insert(
                    dct_msg['object_id'],
                    dct_msg['object_type'],
                    dct_msg['sent_dttm'],
                    json.dumps(dct_msg['payload'])
                )
                # 3. Достаньте `id пользователя` из сообщения
                # и получите полную информацию о пользователе из Redis.
                user_id = dct_msg['payload']['user']['id']
                user_data = self._redis.get(user_id)  # _id, name, login, update_ts_utc
                # 4. Достаньте `id ресторана` из сообщения
                # и получите полную информацию о ресторане из Redis.
                restaurant_id = dct_msg['payload']['restaurant']['id']
                restaurant_data = self._redis.get(restaurant_id)
                # сформируем словарь блюдо-категория из поля "menu" ресторана
                order_item_categories = {}
                menu = restaurant_data['menu']
                for next_item in menu:
                    next_item_id = next_item['_id']
                    next_item_category = next_item['category']
                    order_item_categories[next_item_id] = next_item_category
                # 6. Для каждого `product_id` в сообщении:
                #    1. достать `product_id`.
                #    2. (нужна категория).
                order_items_array = dct_msg['payload']['order_items']
                # order_items: id, name, price, quantity
                # NEED GET category from redis
                products_array = []
                for order_item in order_items_array:
                    order_item_id = order_item['id']
                    if order_item_id in order_item_categories:
                        order_item_category = order_item_categories[order_item_id]
                    else:
                        order_item_category = ''
                    products_array.append(
                        {
                            "id": order_item_id,
                            "name": order_item['name'],
                            "price": order_item['price'],
                            "quantity": order_item['quantity'],
                            "category": order_item_category
                        }
                    )
                # 5. Сформируйте выходное сообщение.
                msg = {
                    'object_id': dct_msg['object_id'],
                    'object_type': dct_msg['object_type'],
                    'payload': {
                        'id': dct_msg['object_id'],
                        'date': dct_msg['payload']['date'],
                        'cost': dct_msg['payload']['cost'],
                        'payment': dct_msg['payload']['payment'],
                        'status': dct_msg['payload']['final_status'],
                        "restaurant": {
                            "id": restaurant_id,
                            "name": restaurant_data['name']
                        },
                        "user": {
                            "id": user_id,
                            "name": user_data['name'],
                            "login": user_data['login']
                        },
                        "products": products_array
                    }
                }
                # 6. Отправьте выходное сообщение в `producer`.
                self._producer.produce(msg)

            processed_messages += 1

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")
