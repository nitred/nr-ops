import collections
import datetime
import itertools
import json
import math as math
import pathlib
import pickle
import random
import uuid
from functools import partial

import numpy as np
import pandas as pd
import requests

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
}
