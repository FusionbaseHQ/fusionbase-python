import os.path

from pkg_resources import DistributionNotFound, get_distribution

from .DataService import DataService
from .DataSource import DataSource
from .DataStream import DataStream
from .DataStreamCollection import DataStreamCollection
from .Fusionbase import Fusionbase

try:
    _dist = get_distribution("fusionbase")
    # Normalize case for Windows systems
    dist_loc = os.path.normcase(_dist.location)
    here = os.path.normcase(__file__)
    if not here.startswith(os.path.join(dist_loc, "fusionbase")):
        raise DistributionNotFound
except DistributionNotFound:
    __version__ = "Please install this project with setup.py"
else:
    __version__ = _dist.version
