# -*- coding: utf-8 -*-
import os


class Env:
    def __init__(self):
        self.region_name = os.getenv("AWS_DEFAULT_REGION")
        assert self.region_name, "Required environment variable `AWS_DEFAULT_REGION` not found."
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        assert self.aws_access_key_id, "Required environment variable `AWS_ACCESS_KEY_ID` not found."
        assert self.aws_secret_access_key, "Required environment variable `AWS_SECRET_ACCESS_KEY` not found."
        self.verify = os.getenv("VERIFY", False)
        self.use_ssl = os.getenv("USE_SSL", True)

ENV = Env()