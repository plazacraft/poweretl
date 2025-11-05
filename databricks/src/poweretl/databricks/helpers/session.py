# pylint: disable=R0913, R0914, R0915, W0212

import json
import os
from dataclasses import dataclass
from typing import Any

from pyspark.sql import SparkSession

from databricks.sdk.dbutils import RemoteDbUtils


@dataclass
class Session:

    dbutils: RemoteDbUtils = None
    display: Any = None
    spark: SparkSession = None


class _MockWidgets:
    def __init__(self):
        self._widgets = {}

    def text(self, name, default, desc=""):  # pylint: disable=W0613
        if name not in self._widgets:
            self._widgets[name] = default

    def get(self, name):
        return self._widgets.get(name, None)

    def remove(self, name):
        self._widgets.pop(name, None)


def get_or_connect(
    *,
    spark=None,
    dbutils=None,
    display=None,
    config_path=None,
    host=None,
    token=None,
    cluster_id=None,
) -> Session:

    def load_config(config_path, host, token, cluster_id) -> dict:
        config = {}
        ret_config = {}
        if host is None or token is None or cluster_id is None:
            if config_path is None:
                module_dir = os.path.dirname(os.path.abspath(__file__))
                config_path = os.path.join(module_dir, ".databricks.config.json")

            if os.path.exists(config_path):
                with open(config_path, "r", encoding="utf-8") as f:
                    config = json.load(f)

        if host is None:
            host = config.get("host", None)
        if token is None:
            token = config.get("token", None)
        if cluster_id is None:
            cluster_id = config.get("cluster_id", None)

        ret_config["host"] = host
        ret_config["token"] = token
        ret_config["cluster_id"] = cluster_id
        return ret_config

    if spark is None:

        try:
            spark = SparkSession.builder.getOrCreate()
        except Exception:  # pylint: disable=W0718, C0415
            from databricks.connect import DatabricksSession  # pylint: disable=C0415

            config = load_config(config_path, host, token, cluster_id)

            if config["cluster_id"] == "serverless":
                spark = DatabricksSession.builder.remote(
                    host=config["host"], token=config["token"], serverless=True
                ).getOrCreate()
            else:
                spark = DatabricksSession.builder.remote(
                    host=config["host"],
                    token=config["token"],
                    cluster_id=config["cluster_id"],
                ).getOrCreate()

            # Patch the destructor to suppress the specific shutdown error
            import threading  # pylint: disable=C0415

            try:
                original_del = threading._DeleteDummyThreadOnDel.__del__

                def safe_del(self):
                    try:
                        original_del(self)
                    except TypeError:
                        pass  # Suppress 'NoneType' context manager error

                threading._DeleteDummyThreadOnDel.__del__ = safe_del
            except AttributeError:
                pass  # If the attribute doesn't exist, do nothing

    if dbutils is None:

        from databricks.sdk import WorkspaceClient  # pylint: disable=C0415

        config = load_config(config_path, host, token, cluster_id)
        if config["host"]:
            workspace_client = WorkspaceClient(
                host=config["host"], token=config["token"]
            )

            dbutils = workspace_client.dbutils

            mock_widgets = _MockWidgets()
            dbutils.widgets.get = mock_widgets.get
            dbutils.widgets.text = mock_widgets.text

    if display is None:

        def display(df):
            if hasattr(df, "show") and callable(getattr(df, "show")):
                df.show()
            else:
                print(df)

    return Session(dbutils=dbutils, display=display, spark=spark)
