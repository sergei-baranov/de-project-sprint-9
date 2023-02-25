import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class DdsRepository:
    _db: PgConnect = None

    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def get_user_category_counters(self, user_pk: str, dict_category_pk: dict) -> list:
        """
        агрегирует заказы посетителя по категориям товаров
        для заданного id пользователя и списка id категорий (все id - строки).
        на выходе - list od dicts; ключи в каждом словаре:
        "h_user_pk", "h_category_pk", "category_name", "order_cnt".
        Если ничего не найдено - на выходе пустой список.
        """

        counters = []

        # dict там на входе - как вариант дедупликации;
        # берём keys(), маппим uuid-ы в строки, джойним
        category_pk_stringified = list(map(lambda x: str(x), dict_category_pk.keys()))
        str_category_pk = "'" + ("', '".join(category_pk_stringified)) + "'"

        query = """
            SELECT
                "u"."h_user_pk",
                "c"."h_category_pk",
                "c"."category_name",
                COUNT(DISTINCT "ou"."h_order_pk") as "order_cnt"
            FROM
                "dds"."h_user" as "u"
                INNER JOIN "dds"."l_order_user" as "ou" ON (
                    "ou"."h_user_pk" = "u"."h_user_pk"
                )
                INNER JOIN "dds"."l_order_product" "op" ON (
                    "op"."h_order_pk" = "ou"."h_order_pk"
                )
                INNER JOIN "dds"."l_product_category" "pc" ON (
                    "pc"."h_product_pk" = "op"."h_product_pk"
                )
                INNER JOIN "dds"."h_category" "c" ON (
                    "c"."h_category_pk" = "pc"."h_category_pk"
                )
            WHERE
                "u"."h_user_pk" = '{filter_user_pk}'
                AND "c"."h_category_pk" IN ({filter_category_pk})
            GROUP BY ("u"."h_user_pk", "c"."h_category_pk", "c"."category_name")
            ;
        """.format(
            filter_user_pk=user_pk,
            filter_category_pk=str_category_pk
        )

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                for record in cur:
                    counters.append({
                        "h_user_pk": str(record[0]),  # 'h_user_pk'
                        "h_category_pk": str(record[1]),  # 'h_category_pk'
                        "category_name": record[2],  # 'category_name'
                        "order_cnt": record[3]  # 'order_cnt'
                    })
                cur.close()

        return counters

    def get_user_product_counters(self, user_pk: str, dict_product_pk: dict) -> list:
        """
        агрегирует заказы посетителя по продуктам (блюдам)
        для заданного id пользователя и списка id продуктов (все id - строки).
        на выходе - list od dicts; ключи в каждом словаре:
        "h_user_pk", "h_product_pk", "product_name", "order_cnt".
        Если ничего не найдено - на выходе пустой список.
        """

        counters = []

        # dict там на входе - как вариант дедупликации;
        # берём keys(), маппим uuid-ы в строки, джойним
        product_pk_stringified = list(map(lambda x: str(x), dict_product_pk.keys()))
        str_product_pk = "'" + ("', '".join(product_pk_stringified)) + "'"

        query = """
            SELECT
                "u"."h_user_pk",
                "op"."h_product_pk",
                "pn"."name" as "product_name",
                COUNT(DISTINCT "ou"."h_order_pk") as "order_cnt"
            FROM
                "dds"."h_user" as "u"
                INNER JOIN "dds"."l_order_user" as "ou" ON (
                    "ou"."h_user_pk" = "u"."h_user_pk"
                )
                INNER JOIN "dds"."l_order_product" "op" ON (
                    "op"."h_order_pk" = "ou"."h_order_pk"
                )
                INNER JOIN "dds"."s_product_names" "pn" ON (
                    "pn"."h_product_pk" = "op"."h_product_pk"
                )
            WHERE
                "u"."h_user_pk" = '{filter_user_pk}'
                AND "op"."h_product_pk" IN ({filter_product_pk})
            GROUP BY ("u"."h_user_pk", "op"."h_product_pk", "pn"."name")
            ;
        """.format(
            filter_user_pk=user_pk,
            filter_product_pk=str_product_pk
        )

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                for record in cur:
                    counters.append({
                        "h_user_pk": str(record[0]),  # 'h_user_pk'
                        "h_product_pk": str(record[1]),  # 'h_product_pk'
                        "product_name": record[2],  # 'product_name'
                        "order_cnt": record[3]  # 'order_cnt'
                    })
                cur.close()

        return counters

    def l_order_user_upsert(self, hk_order_user_pk: str,
                            h_order_pk: str, h_user_pk: str,
                            load_dt: datetime, load_src: str) -> None:
        """upsert l_order_user"""

        # dds.l_order_user
        upsert_statement = """
            INSERT INTO dds.l_order_user
                (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
            VALUES
                (
                    '{upsert_hk_order_user_pk}',
                    '{upsert_h_order_pk}',
                    '{upsert_h_user_pk}',
                    '{upsert_load_dt}',
                    '{upsert_load_src}'
                )
            ON CONFLICT (hk_order_user_pk) DO UPDATE
            SET
                h_order_pk = EXCLUDED.h_order_pk,
                h_user_pk = EXCLUDED.h_user_pk,
                load_dt = EXCLUDED.load_dt,
                load_src = EXCLUDED.load_src
            ;
        """.format(
            upsert_hk_order_user_pk=hk_order_user_pk,
            upsert_h_order_pk=h_order_pk,
            upsert_h_user_pk=h_user_pk,
            upsert_load_dt=load_dt,
            upsert_load_src=load_src
        )
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(upsert_statement)

    def l_order_product_upsert(self, hk_order_product_pk: str,
                                    h_order_pk: str, h_product_pk: str,
                                    load_dt: datetime, load_src: str) -> None:
        """upsert l_order_product"""

        # dds.l_order_product
        upsert_statement = """
            INSERT INTO dds.l_order_product
                (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
            VALUES
                (
                    '{upsert_hk_order_product_pk}',
                    '{upsert_h_order_pk}',
                    '{upsert_h_product_pk}',
                    '{upsert_load_dt}',
                    '{upsert_load_src}'
                )
            ON CONFLICT (hk_order_product_pk) DO UPDATE
            SET
                h_order_pk = EXCLUDED.h_order_pk,
                h_product_pk = EXCLUDED.h_product_pk,
                load_dt = EXCLUDED.load_dt,
                load_src = EXCLUDED.load_src
            ;
        """.format(
            upsert_hk_order_product_pk=hk_order_product_pk,
            upsert_h_order_pk=h_order_pk,
            upsert_h_product_pk=h_product_pk,
            upsert_load_dt=load_dt,
            upsert_load_src=load_src
        )
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(upsert_statement)

    def l_product_restaurant_upsert(self, hk_product_restaurant_pk: str,
                                    h_product_pk: str, h_restaurant_pk: str,
                                    load_dt: datetime, load_src: str) -> None:
        """upsert l_product_restaurant"""

        # dds.l_product_restaurant
        upsert_statement = """
            INSERT INTO dds.l_product_restaurant
                (hk_product_restaurant_pk, h_product_pk, h_restaurant_pk, load_dt, load_src)
            VALUES
                (
                    '{upsert_hk_product_restaurant_pk}',
                    '{upsert_h_product_pk}',
                    '{upsert_h_restaurant_pk}',
                    '{upsert_load_dt}',
                    '{upsert_load_src}'
                )
            ON CONFLICT (hk_product_restaurant_pk) DO UPDATE
            SET
                h_product_pk = EXCLUDED.h_product_pk,
                h_restaurant_pk = EXCLUDED.h_restaurant_pk,
                load_dt = EXCLUDED.load_dt,
                load_src = EXCLUDED.load_src
            ;
        """.format(
            upsert_hk_product_restaurant_pk=hk_product_restaurant_pk,
            upsert_h_product_pk=h_product_pk,
            upsert_h_restaurant_pk=h_restaurant_pk,
            upsert_load_dt=load_dt,
            upsert_load_src=load_src
        )
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(upsert_statement)

    def l_product_category_upsert(self, hk_product_category_pk: str,
                                  h_product_pk: str, h_category_pk: str,
                                  load_dt: datetime, load_src: str) -> None:
        """upsert l_product_category"""

        # dds.l_product_category
        upsert_statement = """
            INSERT INTO dds.l_product_category
                (hk_product_category_pk, h_product_pk, h_category_pk, load_dt, load_src)
            VALUES
                (
                    '{upsert_hk_product_category_pk}',
                    '{upsert_h_product_pk}',
                    '{upsert_h_category_pk}',
                    '{upsert_load_dt}',
                    '{upsert_load_src}'
                )
            ON CONFLICT (hk_product_category_pk) DO UPDATE
            SET
                h_product_pk = EXCLUDED.h_product_pk,
                h_category_pk = EXCLUDED.h_category_pk,
                load_dt = EXCLUDED.load_dt,
                load_src = EXCLUDED.load_src
            ;
        """.format(
            upsert_hk_product_category_pk=hk_product_category_pk,
            upsert_h_product_pk=h_product_pk,
            upsert_h_category_pk=h_category_pk,
            upsert_load_dt=load_dt,
            upsert_load_src=load_src
        )
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(upsert_statement)

    def product_upsert(self, h_product_pk: str, product_id: str,
                       product_name: str,
                       load_dt: datetime, load_src: str) -> None:
        """upsert h_product, s_product_names"""

        # dds.h_product
        upsert_statement = """
            INSERT INTO dds.h_product
                (h_product_pk, product_id, load_dt, load_src)
            VALUES
                (
                    '{upsert_h_product_pk}',
                    '{upsert_product_id}',
                    '{upsert_load_dt}',
                    '{upsert_load_src}'
                )
            ON CONFLICT (h_product_pk) DO UPDATE
            SET
                product_id = EXCLUDED.product_id,
                load_dt = EXCLUDED.load_dt,
                load_src = EXCLUDED.load_src
            ;
        """.format(
            upsert_h_product_pk=h_product_pk,
            upsert_product_id=product_id,
            upsert_load_dt=load_dt,
            upsert_load_src=load_src
        )
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(upsert_statement)

        # dds.s_product_names
        upsert_statement = """
            INSERT INTO dds.s_product_names
                (hk_product_names_pk, h_product_pk, name, load_dt, load_src)
            VALUES
                (
                    '{upsert_hk_product_names_pk}',
                    '{upsert_h_product_pk}',
                    '{upsert_name}',
                    '{upsert_load_dt}',
                    '{upsert_load_src}'
                )
            ON CONFLICT (hk_product_names_pk) DO UPDATE
            SET
                h_product_pk = EXCLUDED.h_product_pk,
                name = EXCLUDED.name,
                load_dt = EXCLUDED.load_dt,
                load_src = EXCLUDED.load_src
            ;
        """.format(
            upsert_hk_product_names_pk=h_product_pk,
            upsert_h_product_pk=h_product_pk,
            upsert_name=product_name,
            upsert_load_dt=load_dt,
            upsert_load_src=load_src
        )
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(upsert_statement)

    def category_upsert(self, h_category_pk: str, category_name: str,
                        load_dt: datetime, load_src: str) -> None:
        """upsert h_category"""

        # dds.h_category
        upsert_statement = """
            INSERT INTO dds.h_category
                (h_category_pk, category_name, load_dt, load_src)
            VALUES
                (
                    '{upsert_h_category_pk}',
                    '{upsert_category_name}',
                    '{upsert_load_dt}',
                    '{upsert_load_src}'
                )
            ON CONFLICT (h_category_pk) DO UPDATE
            SET
                category_name = EXCLUDED.category_name,
                load_dt = EXCLUDED.load_dt,
                load_src = EXCLUDED.load_src
            ;
        """.format(
            upsert_h_category_pk=h_category_pk,
            upsert_category_name=category_name,
            upsert_load_dt=load_dt,
            upsert_load_src=load_src
        )
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(upsert_statement)

    def restaurant_upsert(self, h_restaurant_pk: str, restaurant_id: str,
                          restaurant_name: str,
                          load_dt: datetime, load_src: str) -> None:
        """upsert h_restaurant, s_restaurant_names"""

        # dds.h_restaurant
        upsert_statement = """
            INSERT INTO dds.h_restaurant
                (h_restaurant_pk, restaurant_id, load_dt, load_src)
            VALUES
                (
                    '{upsert_h_restaurant_pk}',
                    '{upsert_restaurant_id}',
                    '{upsert_load_dt}',
                    '{upsert_load_src}'
                )
            ON CONFLICT (h_restaurant_pk) DO UPDATE
            SET
                restaurant_id = EXCLUDED.restaurant_id,
                load_dt = EXCLUDED.load_dt,
                load_src = EXCLUDED.load_src
            ;
        """.format(
            upsert_h_restaurant_pk=h_restaurant_pk,
            upsert_restaurant_id=restaurant_id,
            upsert_load_dt=load_dt,
            upsert_load_src=load_src
        )
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(upsert_statement)

        # dds.s_restaurant_names
        upsert_statement = """
            INSERT INTO dds.s_restaurant_names
                (hk_restaurant_names_pk, h_restaurant_pk, name, load_dt, load_src)
            VALUES
                (
                    '{upsert_hk_restaurant_names_pk}',
                    '{upsert_h_restaurant_pk}',
                    '{upsert_name}',
                    '{upsert_load_dt}',
                    '{upsert_load_src}'
                )
            ON CONFLICT (hk_restaurant_names_pk) DO UPDATE
            SET
                h_restaurant_pk = EXCLUDED.h_restaurant_pk,
                name = EXCLUDED.name,
                load_dt = EXCLUDED.load_dt,
                load_src = EXCLUDED.load_src
            ;
        """.format(
            upsert_hk_restaurant_names_pk=h_restaurant_pk,
            upsert_h_restaurant_pk=h_restaurant_pk,
            upsert_name=restaurant_name,
            upsert_load_dt=load_dt,
            upsert_load_src=load_src
        )
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(upsert_statement)

    def user_upsert(self, h_user_pk: str, user_id: str,
                    username: str, userlogin: str,
                    load_dt: datetime, load_src: str) -> None:
        """upsert h_user, s_user_names"""

        # dds.h_user
        upsert_statement = """
            INSERT INTO dds.h_user
                (h_user_pk, user_id, load_dt, load_src)
            VALUES
                (
                    '{upsert_h_user_pk}',
                    '{upsert_user_id}',
                    '{upsert_load_dt}',
                    '{upsert_load_src}'
                )
            ON CONFLICT (h_user_pk) DO UPDATE
            SET
                user_id = EXCLUDED.user_id,
                load_dt = EXCLUDED.load_dt,
                load_src = EXCLUDED.load_src
            ;
        """.format(
            upsert_h_user_pk=h_user_pk,
            upsert_user_id=user_id,
            upsert_load_dt=load_dt,
            upsert_load_src=load_src
        )
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(upsert_statement)

        # dds.s_user_names
        upsert_statement = """
            INSERT INTO dds.s_user_names
                (hk_user_names_pk, h_user_pk, username, userlogin, load_dt, load_src)
            VALUES
                (
                    '{upsert_hk_user_names_pk}',
                    '{upsert_h_user_pk}',
                    '{upsert_username}',
                    '{upsert_userlogin}',
                    '{upsert_load_dt}',
                    '{upsert_load_src}'
                )
            ON CONFLICT (hk_user_names_pk) DO UPDATE
            SET
                h_user_pk = EXCLUDED.h_user_pk,
                username = EXCLUDED.username,
                userlogin = EXCLUDED.userlogin,
                load_dt = EXCLUDED.load_dt,
                load_src = EXCLUDED.load_src
            ;
        """.format(
            upsert_hk_user_names_pk=h_user_pk,
            upsert_h_user_pk=h_user_pk,
            upsert_username=username,
            upsert_userlogin=userlogin,
            upsert_load_dt=load_dt,
            upsert_load_src=load_src
        )
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(upsert_statement)

    def order_upsert(self, h_order_pk: str, order_id: int, order_dt: datetime,
                     order_cost: float, order_payment: float, order_status: str,
                     load_dt: datetime, load_src: str
                     ) -> None:
        """upsert dds.h_order, s_order_cost, s_order_status"""

        # dds.h_order
        upsert_statement = """
            INSERT INTO dds.h_order
                (h_order_pk, order_id, order_dt, load_dt, load_src)
            VALUES
                (
                    '{upsert_h_order_pk}',
                    {upsert_order_id},
                    '{upsert_order_dt}',
                    '{upsert_load_dt}',
                    '{upsert_load_src}'
                )
            ON CONFLICT (h_order_pk) DO UPDATE
            SET
                order_id = EXCLUDED.order_id,
                order_dt = EXCLUDED.order_dt,
                load_dt = EXCLUDED.load_dt,
                load_src = EXCLUDED.load_src
            ;
        """.format(
            upsert_h_order_pk=h_order_pk,
            upsert_order_id=order_id,
            upsert_order_dt=order_dt,
            upsert_load_dt=load_dt,
            upsert_load_src=load_src
        )
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(upsert_statement)

        # dds.s_order_cost
        upsert_statement = """
            INSERT INTO dds.s_order_cost
                (hk_order_cost_pk, h_order_pk, cost, payment, load_dt, load_src)
            VALUES
                (
                    '{upsert_hk_order_cost_pk}',
                    '{upsert_h_order_pk}',
                    {upsert_cost},
                    {upsert_payment},
                    '{upsert_load_dt}',
                    '{upsert_load_src}'
                )
            ON CONFLICT (hk_order_cost_pk) DO UPDATE
            SET
                h_order_pk = EXCLUDED.h_order_pk,
                cost = EXCLUDED.cost,
                payment = EXCLUDED.payment,
                load_dt = EXCLUDED.load_dt,
                load_src = EXCLUDED.load_src
            ;
        """.format(
            upsert_hk_order_cost_pk=h_order_pk,
            upsert_h_order_pk=h_order_pk,
            upsert_cost=order_cost,
            upsert_payment=order_payment,
            upsert_load_dt=load_dt,
            upsert_load_src=load_src
        )
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(upsert_statement)

        # dds.s_order_status
        upsert_statement = """
            INSERT INTO dds.s_order_status
                (hk_order_status_pk, h_order_pk, status, load_dt, load_src)
            VALUES
                (
                    '{upsert_hk_order_status_pk}',
                    '{upsert_h_order_pk}',
                    '{upsert_status}',
                    '{upsert_load_dt}',
                    '{upsert_load_src}'
                )
            ON CONFLICT (hk_order_status_pk) DO UPDATE
            SET
                h_order_pk = EXCLUDED.h_order_pk,
                status = EXCLUDED.status,
                load_dt = EXCLUDED.load_dt,
                load_src = EXCLUDED.load_src
            ;
        """.format(
            upsert_hk_order_status_pk=h_order_pk,
            upsert_h_order_pk=h_order_pk,
            upsert_status=order_status,
            upsert_load_dt=load_dt,
            upsert_load_src=load_src
        )
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(upsert_statement)

