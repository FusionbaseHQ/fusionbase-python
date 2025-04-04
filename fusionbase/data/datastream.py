"""DataStream module for Fusionbase SDK."""

from typing import Optional

from pydantic import BaseModel


class DataStream(BaseModel):
    """DataStream in the Fusionbase platform.

    DataStreams are machine-readable datasets available in the Fusionbase platform.

    Attributes:
        id: Unique identifier for the data stream
        name: Name of the data stream
        description: Description of the data stream
        owner_id: ID of the data stream owner
        created_at: Creation timestamp
        updated_at: Last update timestamp
        schema: Schema definition of the data
        tags: List of tags
        properties: Additional properties
    """

    @classmethod
    def from_id(cls, client, stream_id: str) -> "DataStream":
        """Create a DataStream instance by fetching it from the API.

        Args:
            client: The Fusionbase client
            stream_id: ID of the data stream to fetch

        Returns:
            A DataStream instance

        Raises:
            APIError: If the data stream cannot be retrieved
        """
        # Implementation will be filled in later
        raise NotImplementedError("DataStream.from_id not implemented yet")

    def get_data(self,
                 limit: Optional[int] = None,
                 offset: Optional[int] = None):
        """Fetch data from this data stream.

        Args:
            limit: Maximum number of records to fetch
            offset: Number of records to skip

        Returns:
            List of data records

        Raises:
            APIError: If the data cannot be retrieved
        """
        # Implementation will be filled in later
        raise NotImplementedError("DataStream.get_data not implemented yet")
