# pylint: disable=R0914, W0718, R0912, R0915, R1724, W0612, C0415

import json
from typing import Any, Dict, List

from poweretl.common.providers.base_meta_provider import BaseMetaProvider
from poweretl.defs import Meta, Model
from poweretl.defs.meta import BaseItem as MetaBaseItem
from poweretl.defs.meta import Column as MetaColumn
from poweretl.defs.meta import MetaInfo
from poweretl.defs.meta import NameValue as MetaNameValue
from poweretl.defs.meta import Table as MetaTable
from poweretl.defs.meta import Tables as MetaTables
from pyspark.sql import SparkSession  # pylint: disable=C0411


class DbxMetaProvider(BaseMetaProvider):

    def __init__(self, spark: SparkSession, catalog: str, schema: str):
        super().__init__()
        self._spark = spark
        self._catalog = catalog
        self._schema = schema

    # -----------------------------
    # Public API (IMetaProvider)
    # -----------------------------
    def self_update(self):
        return

    def push_model_changes(self, model: Model):
        current = self.get_meta()
        updated = self._get_updated_meta(model, current)
        self._save_meta(updated)

    def push_meta_changes(self, meta: Meta):
        current = self.get_meta()
        # Merge current + provided meta to get final state
        # Using the same helper as FileMetaProvider
        from deepmerge import (
            always_merger,  # lazy import to avoid hard dependency at module import
        )

        merged = always_merger.merge(current, meta)
        self._save_meta(merged)

    def push_meta_item_changes(self, item: MetaBaseItem):
        current = self.get_meta()
        item_to_update = self._find_by_object_id(current, item)
        from poweretl.utils import DataclassUpgrader  # lazy import

        upgrader = DataclassUpgrader(MetaBaseItem)
        upgrader.update_child(item, item_to_update)
        self._save_meta(current)

    def get_meta(self, statuses: set[str] = None, table_id: str = None) -> Meta:
        rows = self._read_meta_rows()
        meta = self._build_meta_from_rows(rows)

        if table_id and meta:
            meta.tables.items = {
                k: v for k, v in meta.tables.items.items() if k == table_id
            }

        if statuses and meta:
            meta = self._apply_status_filter(meta, statuses)

        return meta

    # -----------------------------
    # Internal helpers
    # -----------------------------
    def _meta_table_fullname(self) -> str:
        return f"{self._catalog}.{self._schema}.meta"

    def _read_meta_rows(self) -> List[Dict[str, Any]]:
        tbl = self._meta_table_fullname()
        df = self._spark.sql(
            f"""
            SELECT object_id, status, operation, error_msg, updated_fields,
                   model_last_update, meta_last_update, parent_object_id,
                   type, name, linked, data
            FROM {tbl}
            """
        )
        return [r.asDict(recursive=True) for r in df.collect()]

    def _build_meta_from_rows(self, rows: List[Dict[str, Any]]) -> Meta:
        tables: Dict[str, MetaTable] = {}
        columns_by_parent: Dict[str, Dict[str, MetaColumn]] = {}

        # Pass 1: tables
        for r in rows:
            if r.get("type") != "table":
                continue
            data = self._safe_json_loads(r.get("data")) or {}

            minfo = MetaInfo(
                object_id=r.get("object_id"),
                status=r.get("status"),
                operation=r.get("operation"),
                error_msg=r.get("error_msg"),
                updated_fields=self._safe_json_loads(r.get("updated_fields")) or {},
                model_last_update=r.get("model_last_update"),
                meta_last_update=r.get("meta_last_update"),
            )
            t = MetaTable(
                name=r.get("name"),
                linked=bool(r.get("linked")) if r.get("linked") is not None else False,
                external_location=data.get("external_location"),
                comment=data.get("comment"),
                meta=minfo,
            )
            tables[minfo.object_id] = t
            columns_by_parent[minfo.object_id] = {}

        # Pass 2: columns
        for r in rows:
            if r.get("type") != "table_column":
                continue
            parent_id = r.get("parent_object_id")
            if parent_id not in tables:
                # skip orphans
                continue
            data = self._safe_json_loads(r.get("data")) or {}
            minfo = MetaInfo(
                object_id=r.get("object_id"),
                status=r.get("status"),
                operation=r.get("operation"),
                error_msg=r.get("error_msg"),
                updated_fields=self._safe_json_loads(r.get("updated_fields")) or {},
                model_last_update=r.get("model_last_update"),
                meta_last_update=r.get("meta_last_update"),
            )
            c = MetaColumn(
                name=r.get("name"),
                linked=bool(r.get("linked")) if r.get("linked") is not None else False,
                type=data.get("type"),
                comment=data.get("comment"),
                meta=minfo,
            )
            columns_by_parent[parent_id][minfo.object_id] = c

        # Attach columns to tables
        for table_id, t in tables.items():
            t.columns.items = columns_by_parent.get(table_id, {})

        # Pass 3: name-value like items (tags/properties/settings, column tags)
        for r in rows:
            rtype = r.get("type")
            if rtype not in {
                "table_tag",
                "table_property",
                "table_setting",
                "column_tag",
            }:
                continue
            data = self._safe_json_loads(r.get("data")) or {}
            value = data.get("value")
            name = r.get("name")
            parent_id = r.get("parent_object_id")
            if rtype == "column_tag":
                # find column by object id (parent is column id)
                # locate the table that owns this column
                for t in tables.values():
                    if parent_id in t.columns.items:
                        t.columns.items[parent_id].tags.items[name] = MetaNameValue(
                            name=name,
                            linked=False,
                            meta=MetaInfo(object_id=r.get("object_id")),
                            value=value,
                        )
                        break
            else:
                # parent is table id
                if parent_id in tables:
                    target_table = tables[parent_id]
                    nv = MetaNameValue(
                        name=name,
                        linked=False,
                        meta=MetaInfo(object_id=r.get("object_id")),
                        value=value,
                    )
                    if rtype == "table_tag":
                        target_table.tags.items[name] = nv
                    elif rtype == "table_property":
                        target_table.properties.items[name] = nv
                    elif rtype == "table_setting":
                        target_table.settings.items[name] = nv

        meta = Meta(tables=MetaTables(items=tables))
        return meta

    def _flatten_meta_to_rows(self, meta: Meta) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []

        def _minfo_dict(mi: MetaInfo) -> Dict[str, Any]:
            return {
                "object_id": mi.object_id,
                "status": mi.status,
                "operation": mi.operation,
                "error_msg": mi.error_msg,
                "updated_fields": json.dumps(mi.updated_fields or {}),
                "model_last_update": mi.model_last_update,
                "meta_last_update": mi.meta_last_update,
            }

        # Tables
        for table_id, t in meta.tables.items.items():
            mi = t.meta
            data = {
                "external_location": t.external_location,
                "comment": t.comment,
            }
            row = {
                **_minfo_dict(mi),
                "parent_object_id": None,
                "type": "table",
                "name": t.name,
                "linked": bool(t.linked),
                "data": json.dumps(data),
            }
            rows.append(row)

            # Columns
            for col_id, c in t.columns.items.items():
                cmi = c.meta
                cdata = {"type": c.type, "comment": c.comment}
                crow = {
                    **_minfo_dict(cmi),
                    "parent_object_id": mi.object_id,
                    "type": "table_column",
                    "name": c.name,
                    "linked": bool(c.linked),
                    "data": json.dumps(cdata),
                }
                rows.append(crow)

                # Column tags
                for tag_id, tag in c.tags.items.items():
                    tagmi = tag.meta
                    trow = {
                        **_minfo_dict(tagmi),
                        "parent_object_id": cmi.object_id,
                        "type": "column_tag",
                        "name": tag.name,
                        "linked": bool(tag.linked),
                        "data": json.dumps({"value": tag.value}),
                    }
                    rows.append(trow)

            # Table tags
            for tag_id, tag in t.tags.items.items():
                tagmi = tag.meta
                trow = {
                    **_minfo_dict(tagmi),
                    "parent_object_id": mi.object_id,
                    "type": "table_tag",
                    "name": tag.name,
                    "linked": bool(tag.linked),
                    "data": json.dumps({"value": tag.value}),
                }
                rows.append(trow)

            # Table properties
            for prop_id, prop in t.properties.items.items():
                pmi = prop.meta
                prow = {
                    **_minfo_dict(pmi),
                    "parent_object_id": mi.object_id,
                    "type": "table_property",
                    "name": prop.name,
                    "linked": bool(prop.linked),
                    "data": json.dumps({"value": prop.value}),
                }
                rows.append(prow)

            # Table settings
            for set_id, setting in t.settings.items.items():
                smi = setting.meta
                srow = {
                    **_minfo_dict(smi),
                    "parent_object_id": mi.object_id,
                    "type": "table_setting",
                    "name": setting.name,
                    "linked": bool(setting.linked),
                    "data": json.dumps({"value": setting.value}),
                }
                rows.append(srow)

        return rows

    def _save_meta(self, meta: Meta):
        rows = self._flatten_meta_to_rows(meta)
        tbl = self._meta_table_fullname()

        # Truncate and insert all (similar to writing whole file in FileMetaProvider)
        self._spark.sql(f"TRUNCATE TABLE {tbl}")

        if not rows:
            return

        df = self._spark.createDataFrame(rows)
        df.createOrReplaceTempView("_tmp_poweretl_meta_rows")
        self._spark.sql(
            f"""
            INSERT INTO {tbl}
            SELECT
                object_id, status, operation, error_msg, updated_fields,
                model_last_update, meta_last_update, parent_object_id,
                type, name, linked, data
            FROM _tmp_poweretl_meta_rows
            """
        )

    @staticmethod
    def _safe_json_loads(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(value)
        except Exception:
            return None
