# -*- coding: utf-8 -*-
import os
from distutils.util import strtobool

class Env:
    def __init__(self):
        _use_local_ddb = os.getenv("USE_LOCAL_DDB", None)
        assert _use_local_ddb is not None, "Required environment variable `USE_LOCAL_DDB` not found."

        self.use_local_ddb = bool(strtobool(_use_local_ddb))

        if self.use_local_ddb:
            self.endpoint_url = os.getenv("LOCAL_DDB_ENDPOINT_URL")
            assert self.endpoint_url, "Required environment variable `LOCAL_DB_ENDPOINT_URL` not found."
        else:
            self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
            self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
            assert self.aws_access_key_id, "Required environment variable `AWS_ACCESS_KEY_ID` not found."
            assert self.aws_secret_access_key, "Required environment variable `AWS_SECRET_ACCESS_KEY` not found."
            self.verify = os.getenv("VERIFY", False)
            self.use_ssl = os.getenv("USE_SSL", True)

        self.region_name = os.getenv("AWS_DEFAULT_REGION")
        assert self.region_name, "Required environment variable `AWS_DEFAULT_REGION` not found."

ENV = Env()