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
import sklearn
import sklearn.datasets
import sklearn.linear_model
import sklearn.metrics
import sklearn.model_selection
import sklearn.pipeline
import sklearn.preprocessing
import sklearn.svm

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
    # sklearn
    "sklearn": sklearn,
    "sklearn.datasets": sklearn.datasets,
    "sklearn.linear_model": sklearn.linear_model,
    "sklearn.metrics": sklearn.metrics,
    "sklearn.model_selection": sklearn.model_selection,
    "sklearn.pipeline": sklearn.pipeline,
    "sklearn.preprocessing": sklearn.preprocessing,
    "sklearn.svm": sklearn.svm,
}
