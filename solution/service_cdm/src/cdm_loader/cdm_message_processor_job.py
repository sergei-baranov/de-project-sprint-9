import uuid
from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer
from lib.pg import PgConnect
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    _kafka_consumer: KafkaConsumer = None
    _cdm_repository: CdmRepository = None
    _logger: Logger = None
    _batch_size: int = 100

    def __init__(self,
                 kafka_consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 logger: Logger) -> None:
        self._kafka_consumer = kafka_consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        # forced
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        processed_messages = 0;
        timeout: float = 3.0
        while processed_messages < self._batch_size:
            # Step 1. Получаем сообщение из Kafka с помощью `consume()`.
            dct_msg = self._kafka_consumer.consume(timeout=timeout)
            # message example (пример реализованного контракта)
            # одно сообщение 'object_type': 'user_product_counters'
            # {
            #   'object_id': 'dd2854ab-fdf5-44a7-9682-da9317418577',
            #   'object_type': 'user_product_counters',
            #   'payload': {
            #     'id': 'dd2854ab-fdf5-44a7-9682-da9317418577',
            #     'counters': [
            #       {
            #         'h_user_pk': '1058719d-c8f0-3a48-ad7a-87d1678fdf8e',
            #         'h_product_pk': '37ec848f-bad1-3172-a92d-9386b9616689',
            #         'product_name': 'Вода Свирме',
            #         'order_cnt': 1
            #       },
            #       ...
            #     ]
            #   }
            # }
            # и одно сообщение 'object_type': 'user_category_counters'
            # {
            #   'object_id': '32ed3f22-43a7-415a-89a5-4de5822e9991',
            #   'object_type': 'user_category_counters',
            #   'payload': {
            #     'id': '32ed3f22-43a7-415a-89a5-4de5822e9991',
            #     'counters': [
            #       {
            #         'h_user_pk': '1058719d-c8f0-3a48-ad7a-87d1678fdf8e',
            #         'h_category_pk': '11a15ec4-37ae-3363-878f-c9a2cd7249b5',
            #         'category_name': 'Выпечка',
            #         'order_cnt': 1
            #       },
            #       ...
            #     ]
            #   }
            # }

            # а вот и случай, когда у нас в топике сообщения разных типов:
            if 'object_type' not in dct_msg:
                continue
            if (
                dct_msg['object_type'] != 'user_product_counters'
                and dct_msg['object_type'] != 'user_category_counters'
            ):
                continue

            if dct_msg['object_type'] == 'user_product_counters':
                # upsert user_product_counters
                for next_counter in dct_msg['payload']['counters']:
                    h_user_pk = next_counter['h_user_pk']
                    h_product_pk = next_counter['h_product_pk']
                    product_name = next_counter['product_name']
                    order_cnt = next_counter['order_cnt']
                    self._cdm_repository.user_product_counters_upsert(
                        h_user_pk, h_product_pk, product_name, order_cnt
                    )
            elif dct_msg['object_type'] == 'user_category_counters':
                # upsert user_category_counters
                for next_counter in dct_msg['payload']['counters']:
                    h_user_pk = next_counter['h_user_pk']
                    h_category_pk = next_counter['h_category_pk']
                    category_name = next_counter['category_name']
                    order_cnt = next_counter['order_cnt']
                    self._cdm_repository.user_category_counters_upsert(
                        h_user_pk, h_category_pk, category_name, order_cnt
                    )
            else:
                pass

        self._logger.info(f"{datetime.utcnow()}: FINISH")
