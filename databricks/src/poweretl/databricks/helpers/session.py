from dataclasses import dataclass
from pyspark.sql.functions import array
from pyspark.sql import SparkSession

import os
import json
import base64

@dataclass
class Session:
    dbutils = None
    display = None  
    spark = None

class _MockWidgets:
    def __init__(self):
        self._widgets = {}

    def text(self, name, default, desc = ""):
        if (name not in self._widgets):
            self._widgets[name] = default

    def get(self, name):
        return self._widgets.get(name, None)

    def remove(self, name):
        self._widgets.pop(name, None)


def get_or_connect(*, 
                          spark = None, 
                          dbutils = None, 
                          display = None, 
                          config_path = None,
                          host = None ,
                          token = None,
                          cluster_id=None) -> Session:

    if spark is None:
        try:
            spark = SparkSession.builder.getOrCreate()
        except:
            # cannot be imported at te beginning of the script because it will not run from dbx workspace
            from databricks.connect import DatabricksSession
            from databricks.sdk import WorkspaceClient

            config_path = config_path
            config = {}
            if host is None or token is None or cluster_id is None:
                if config_path is None:
                    module_dir = os.path.dirname(os.path.abspath(__file__))
                    config_path = os.path.join(module_dir, ".databricks.config.json")

                with open(config_path, "r") as f:
                    config = json.load(f)

            if (host is None):
                host =  config["host"]
            if (token is None):
                token = config["token"]
            if (cluster_id is None):
                cluster_id = config["cluster_id"]
    
    
            if (cluster_id == "serverless"):
                spark = DatabricksSession.builder.remote( host = host, token = token, serverless=True).getOrCreate()
            else:
                spark = DatabricksSession.builder.remote( host = host, token = token, cluster_id=cluster_id).getOrCreate()

            #Patch the destructor to suppress the specific shutdown error
            import threading
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


    if (dbutils is None):

        workspace_client = WorkspaceClient(host=host, token=token)

        dbutils = workspace_client.dbutils

        mock_widgets =  _MockWidgets()
        dbutils.widgets.get = mock_widgets.get
        dbutils.widgets.text = mock_widgets.text

    if (display is None):    
        def display(df):
            if hasattr(df, "show") and callable(getattr(df, "show")):
                df.show()
            else:
                print(df)

    return Session(
        dbutils=dbutils,
        display=display,
        spark=spark
    )

