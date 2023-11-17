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
import math
import pathlib
import pickle
import pprint
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
import sklearn
import sklearn.datasets
import sklearn.linear_model
import sklearn.metrics
import sklearn.model_selection
import sklearn.pipeline
import sklearn.preprocessing
import sklearn.svm

from nr_ops.utils.eval.custom_functions import custom_raise_exception

logger = logging.getLogger(__name__)


EVAL_GLOBALS = {
    "base64": base64,
    "collections": collections,
    "copy": copy,
    "datetime": datetime,
    "gzip": gzip,
    "hashlib": hashlib,
    "importlib": importlib,
    "io": io,
    "itertools": itertools,
    "json": json,
    "logging": logging,
    "math": math,
    "pathlib": pathlib,
    "pickle": pickle,
    "pprint": pprint,
    "random": random,
    "re": re,
    "shutil": shutil,
    "tarfile": tarfile,
    "time": time,
    "uuid": uuid,
    "zipfile": zipfile,
    "partial": partial,
    # third-party
    "np": np,
    "pd": pd,
    "requests": requests,
    # third-party: sklearn
    "sklearn": sklearn,
    "sklearn.datasets": sklearn.datasets,
    "sklearn.linear_model": sklearn.linear_model,
    "sklearn.metrics": sklearn.metrics,
    "sklearn.model_selection": sklearn.model_selection,
    "sklearn.pipeline": sklearn.pipeline,
    "sklearn.preprocessing": sklearn.preprocessing,
    "sklearn.svm": sklearn.svm,
    # custom_functions
    "custom_raise_exception": custom_raise_exception,
}
