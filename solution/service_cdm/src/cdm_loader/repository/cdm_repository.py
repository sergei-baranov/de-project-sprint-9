import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class CdmRepository:
    _db: PgConnect = None

    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def user_category_counters_upsert(self,
                                      h_user_pk: str, h_category_pk: str,
                                      category_name: str, order_cnt: int) -> None:
        """upsert user_category_counters"""

        upsert_statement = """
            INSERT INTO cdm.user_category_counters
                (user_id, category_id, category_name, order_cnt)
            VALUES
                (
                    '{upsert_h_user_pk}',
                    '{upsert_h_category_pk}',
                    '{upsert_category_name}',
                    {upsert_order_cnt}
                )
            ON CONFLICT (user_id, category_id) DO UPDATE
            SET
                category_name = EXCLUDED.category_name,
                order_cnt = EXCLUDED.order_cnt
            ;
        """.format(
            upsert_h_user_pk=h_user_pk,
            upsert_h_category_pk=h_category_pk,
            upsert_category_name=category_name,
            upsert_order_cnt=order_cnt
        )
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(upsert_statement)

    def user_product_counters_upsert(self,
                                     h_user_pk: str, h_product_pk: str,
                                     product_name: str, order_cnt: int) -> None:
        """upsert user_product_counters"""

        upsert_statement = """
            INSERT INTO cdm.user_product_counters
                (user_id, product_id, product_name, order_cnt)
            VALUES
                (
                    '{upsert_h_user_pk}',
                    '{upsert_h_product_pk}',
                    '{upsert_product_name}',
                    {upsert_order_cnt}
                )
            ON CONFLICT (user_id, product_id) DO UPDATE
            SET
                product_name = EXCLUDED.product_name,
                order_cnt = EXCLUDED.order_cnt
            ;
        """.format(
            upsert_h_user_pk=h_user_pk,
            upsert_h_product_pk=h_product_pk,
            upsert_product_name=product_name,
            upsert_order_cnt=order_cnt
        )
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(upsert_statement)
