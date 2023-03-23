import base64
import collections
import copy
import datetime
import gzip
import hashlib
import importlib
import io
import itertools
import json
import logging
import math as math
import pathlib
import pickle
import random
import re
import shutil
import tarfile
import time
import uuid
import zipfile
from functools import partial

import numpy as np
import pandas as pd
import requests

logger = logging.getLogger(__name__)

EVAL_GLOBALS = {
    "random": random,
    "datetime": datetime,
    "math": math,
    "np": np,
    "pathlib": pathlib,
    "pd": pd,
    "pickle": pickle,
    "uuid": uuid,
    "partial": partial,
    "collections": collections,
    "json": json,
    "itertools": itertools,
    "requests": requests,
    "logger": logger,
    "time": time,
    "base64": base64,
    "copy": copy,
    "gzip": gzip,
    "hashlib": hashlib,
    "importlib": importlib,
    "io": io,
    "re": re,
    "shutil": shutil,
    "tarfile": tarfile,
    "zipfile": zipfile,
}
