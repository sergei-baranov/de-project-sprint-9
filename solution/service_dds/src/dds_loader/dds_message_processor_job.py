import uuid
from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.pg import PgConnect
from dds_loader.repository.dds_repository import DdsRepository


class DdsMessageProcessor:
    _kafka_consumer: KafkaConsumer = None
    _kafka_producer: KafkaProducer = None
    _dds_repository: DdsRepository = None
    _logger: Logger = None
    _batch_size: int = 30

    def __init__(self,
                 kafka_consumer: KafkaConsumer,
                 kafka_producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 logger: Logger) -> None:

        self._kafka_consumer = kafka_consumer
        self._kafka_producer = kafka_producer
        self._dds_repository = dds_repository
        self._logger = logger
        # forced
        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        processed_messages = 0;
        timeout: float = 3.0
        while processed_messages < self._batch_size:
            # Step 1. Получаем сообщение из Kafka с помощью `consume()`.
            dct_msg = self._kafka_consumer.consume(timeout=timeout)
            # message example (пример реализованного контракта)
            # {
            # 'object_id': 1027424,
            # 'object_type': 'order',
            # 'payload': {
            #   'id': 1027424,
            #   'date': '2023-02-16 21:02:37',
            #   'cost': 1600,
            #   'payment': 1600,
            #   'status': 'CLOSED',
            #   'restaurant': {
            #     'id': 'a51e4e31ae4602047ec52534',
            #     'name': 'Кубдари'
            #   },
            #   'user': {
            #     'id': '626a81ce9a8cd1920641e275',
            #     'name': 'Светлана Валериевна Корнилова'
            #     'login': 'izmail28'
            #   },
            #   'products': [
            #     {
            #       'id': '1f22499787cfda2be82a71e7',
            #       'name': 'Оджахури с грибами и зеленью',
            #       'price': 400,
            #       'quantity': 4,
            #       'category': 'Основные блюда'
            #     }
            #   ]
            # }
            # }

            # на случай, когда у нас будут в топике сообщения разных типов:
            if 'object_type' not in dct_msg:
                continue
            if dct_msg['object_type'] != 'order':
                continue

            # источник у нас один - это мы и есть,
            # согласно предложению в уроке будет "orders-system-kafka"
            load_src = "orders-system-kafka"

            # load_dt
            # с таймстемпами в постгресе "всё сложно",
            # но раз в уроках велели создать обычные timestamp-поля,
            # то вместо now() юзаем utcnow() (?)
            load_dt = datetime.utcnow()

            # Step 2. upsert dds.h_order, s_order_cost, s_order_status
            order_id = dct_msg['payload']['id']
            h_order_pk = uuid.uuid3(uuid.NAMESPACE_X500, str(order_id))
            order_dt = dct_msg['payload']['date']
            order_cost = dct_msg['payload']['cost']
            order_payment = dct_msg['payload']['payment']
            order_status = dct_msg['payload']['status']
            self._dds_repository.order_upsert(
                h_order_pk, order_id, order_dt,
                order_cost, order_payment, order_status,
                load_dt, load_src
            )

            # Step 3. upsert h_user, s_user_names
            user_id = dct_msg['payload']['user']['id']
            h_user_pk = uuid.uuid3(uuid.NAMESPACE_X500, user_id)
            username = dct_msg['payload']['user']['name']
            userlogin = username
            if 'login' in dct_msg['payload']['user']:
                userlogin = dct_msg['payload']['user']['login']
            self._dds_repository.user_upsert(
                h_user_pk, user_id, username, userlogin,
                load_dt, load_src
            )

            # Step 4. upsert h_restaurant, s_restaurant_names
            restaurant_id = dct_msg['payload']['restaurant']['id']
            h_restaurant_pk = uuid.uuid3(uuid.NAMESPACE_X500, restaurant_id)
            restaurant_name = dct_msg['payload']['restaurant']['name']
            self._dds_repository.restaurant_upsert(
                h_restaurant_pk, restaurant_id, restaurant_name,
                load_dt, load_src
            )

            # prepare products and categories
            upsert_products = {}
            upsert_categories = {}
            for next_product in dct_msg['payload']['products']:
                next_category = next_product['category']
                upsert_categories[next_category] = next_category
                next_product_id = next_product['id']
                next_product_name = next_product['name']
                upsert_products[next_product_id] = next_product_name

            # Step 5. upsert h_category
            for next_category in upsert_categories:
                h_category_pk = uuid.uuid3(uuid.NAMESPACE_X500, next_category)
                self._dds_repository.category_upsert(
                    h_category_pk, next_category, load_dt, load_src)

            # Step 6. upsert h_product, s_product_names
            for next_product_id in upsert_products:
                next_product_name = upsert_products[next_product_id]
                h_product_pk = uuid.uuid3(uuid.NAMESPACE_X500, next_product_id)
                self._dds_repository.product_upsert(
                    h_product_pk, next_product_id, next_product_name,
                    load_dt, load_src
                )

            # для наглядности отдельный цикл с повторным формированием uuid и т. п.
            #
            # сюда сложим pk всех продуктов для дальнейшего использования
            # в Step 11. Сообщения для cdm-сервиса, а именно для заполнения cdm.user_product_counters
            dict_product_pk = {}
            # сюда сложим pk всех категорий для дальнейшего использования
            # в Step 12. Сообщения для cdm-сервиса, а именно для заполненияcdm.user_category_counters
            dict_category_pk = {}
            for next_product in dct_msg['payload']['products']:
                next_product_id = next_product['id']
                h_product_pk = uuid.uuid3(uuid.NAMESPACE_X500, next_product_id)
                dict_product_pk[h_product_pk] = h_product_pk  # for Step 11

                next_category = next_product['category']
                h_category_pk = uuid.uuid3(uuid.NAMESPACE_X500, next_category)
                dict_category_pk[h_category_pk] = h_category_pk  # for Step 12

                # Step 7. upsert l_product_category
                hk_product_category_pk = uuid.uuid3(
                    uuid.NAMESPACE_X500,
                    str(h_product_pk) + '/' + str(h_category_pk)
                )
                self._dds_repository.l_product_category_upsert(
                    hk_product_category_pk,
                    h_product_pk, h_category_pk,
                    load_dt, load_src
                )

                # Step 8. upsert l_product_restaurant
                # h_restaurant_pk - see Step 4 above
                # h_product_pk - see Step 7 above
                hk_product_restaurant_pk = uuid.uuid3(
                    uuid.NAMESPACE_X500,
                    str(h_product_pk) + '/' + str(h_restaurant_pk)
                )
                self._dds_repository.l_product_restaurant_upsert(
                    hk_product_restaurant_pk,
                    h_product_pk, h_restaurant_pk,
                    load_dt, load_src
                )

                # Step 9. upsert l_order_product
                # h_order_pk - see Step 2 above
                # h_product_pk - see Step 7 above
                hk_order_product_pk = uuid.uuid3(
                    uuid.NAMESPACE_X500,
                    str(h_order_pk) + '/' + str(h_product_pk)
                )
                self._dds_repository.l_order_product_upsert(
                    hk_order_product_pk,
                    h_order_pk, h_product_pk,
                    load_dt, load_src
                )

            # Step 10. upsert l_order_user
            # h_order_pk - see Step 2 above
            # h_user_pk - see Step 3 above
            hk_order_user_pk = uuid.uuid3(
                uuid.NAMESPACE_X500,
                str(h_order_pk) + '/' + str(h_user_pk)
            )
            self._dds_repository.l_order_user_upsert(
                hk_order_user_pk,
                h_order_pk, h_user_pk,
                load_dt, load_src
            )

            # Step 11. Сообщения для cdm-сервиса, а именно для заполнения cdm.user_product_counters
            # (счётчик заказов посетителя по продуктам (блюдам)).
            # Я думаю, делать надо так: агрегирую данные для пользователя и продуктов,
            # которые поучаствовали в данном заказе (то есть данные в БД были изменены),
            # и отправляю сообщение, а в нём в payload/counters: list,
            # указывая тип сообщения (object_type) для обработчика - 'user_product_counters'.
            # Далее cdm-service тоже будет не инкрементить, а тупо апсертить данные в витрины,
            # после отработки каждого заказа, так надёжнее.
            list_dicts_product_counters = self._dds_repository.get_user_product_counters(h_user_pk, dict_product_pk)
            random_uuid = uuid.uuid4()
            msg_type = 'user_product_counters'
            msg = {
                'object_id': str(random_uuid),
                'object_type': msg_type,
                'payload': {
                    'id': str(random_uuid),
                    "counters": list_dicts_product_counters
                }
            }
            self._kafka_producer.produce(msg)

            # Step 12. Сообщения для cdm-сервиса, а именно для заполненияcdm.user_category_counters
            # (счётчик заказов посетителя по категориям товаров).
            # Я думаю, делать надо так: агрегирую данные для пользователя и категорий,
            # которые поучаствовали в данном заказе (то есть данные в БД были изменены),
            # и отправляю сообщение, а в нём в payload/counters: list,
            # указывая тип сообщения (object_type) для обработчика - 'user_category_counters'.
            # Далее cdm-service тоже будет не инкрементить, а тупо апсертить данные в витрины,
            # после отработки каждого заказа, так надёжнее.
            list_dicts_category_counters = self._dds_repository.get_user_category_counters(h_user_pk, dict_category_pk)
            random_uuid = uuid.uuid4()
            msg_type = 'user_category_counters'
            msg = {
                'object_id': str(random_uuid),
                'object_type': msg_type,
                'payload': {
                    'id': str(random_uuid),
                    "counters": list_dicts_category_counters
                }
            }
            self._kafka_producer.produce(msg)

            # Step 13. Чистимся.
            # не знаю, ндо ли в питоне следить за этим,
            # но всё же мы резидентом висим в контейнере...
            del dct_msg, msg, list_dicts_product_counters, list_dicts_category_counters, \
                dict_product_pk, dict_category_pk, upsert_products, upsert_categories

        self._logger.info(f"{datetime.utcnow()}: FINISH")
