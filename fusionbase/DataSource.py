import requests
from rich.console import Console


class DataSource:
    def __init__(self, auth: dict, connection: dict, log: bool = False) -> None:
        """
       Used to initialise a new DataSource Object
       :param auth: the standard authentication object to authenticate yourself towards the fusionbase API
       Example:
       auth = {"api_key": " ***** Hidden credentials *****"}

       :param connection: the standard authentication object used to verify e.g which uri should be used
       Example:
       connection={"base_uri": "https://api.fusionbase.com/api/v1"}

       :param log: Whether the the output of any given operation should be logged to console
       """
        self.auth = auth
        self.connection = connection
        self.base_uri = self.connection["base_uri"]
        self.requests = requests.Session()
        self.requests.headers.update({"x-api-key": self.auth["api_key"]})
        self.log = log
        self.console = Console()

    def _log(self, message, force=False):
        if not self.log and not force:
            return None
        else:
            self.console.log(message)

    def get_all(self) -> list:
        """
        Used to retrieve all DataSources
        :return:  All Datasource's provided by the Fusionbase API
        """
        r = self.requests.get(f"{self.base_uri}/data-source/get/all")

        if r.status_code == 200:
            self._log("✅ [green]Successfully retrieved all data sources.[/green]")
            return r.json()

        return None
