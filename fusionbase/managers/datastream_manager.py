"""DataStream manager for Fusionbase SDK."""

from typing import Dict, List

from fusionbase.data.datastream import DataStream
from fusionbase.exceptions import APIError
from fusionbase.exceptions import ResourceNotFoundError


class DataStreamManager:
    """Manager for DataStream operations.

    This class provides methods to retrieve and interact with DataStreams.
    """

    def __init__(self, client):
        """Initialize the DataStream manager with a client.

        Args:
            client: A Fusionbase client instance
        """
        self._client = client
        self._streams_cache: Dict[str, DataStream] = {}

    def from_id(self, stream_id: str, validate: bool = True) -> DataStream:
        """Get a DataStream by ID.

        Args:
            stream_id: ID of the stream to retrieve
            validate: Whether to validate if the stream exists (default: True)

        Returns:
            DataStream instance

        Raises:
            ResourceNotFoundError: If validate=True and the stream doesn't exist
        """
        # Check if we already have this stream in cache
        if stream_id in self._streams_cache:
            return self._streams_cache[stream_id]

        # Create a new DataStream instance
        stream = DataStream(self._client, stream_id)

        # Validate that the stream exists by fetching its metadata
        if validate:
            try:
                stream.get_metadata()
            except ResourceNotFoundError as e:
                # Stream doesn't exist - don't cache and re-raise
                raise ResourceNotFoundError("data_stream", stream_id,
                                            getattr(e, "response", None)) from e
            except Exception:
                # Another error occurred, but we'll still create the stream
                # Just don't validate it was successful
                pass

        # Store in cache
        self._streams_cache[stream_id] = stream

        return stream

    async def afrom_id(self,
                       stream_id: str,
                       validate: bool = True) -> DataStream:
        """Asynchronously get a DataStream by ID.

        Args:
            stream_id: ID of the stream to retrieve
            validate: Whether to validate if the stream exists (default: True)

        Returns:
            DataStream instance

        Raises:
            ResourceNotFoundError: If validate=True and the stream doesn't exist
        """
        # Check if we already have this stream in cache
        if stream_id in self._streams_cache:
            return self._streams_cache[stream_id]

        # Create a new DataStream instance
        stream = DataStream(self._client, stream_id)

        # Validate that the stream exists by fetching its metadata
        if validate:
            try:
                await stream.aget_metadata()
            except ResourceNotFoundError as e:
                # Stream doesn't exist - don't cache and re-raise
                raise ResourceNotFoundError("data_stream", stream_id,
                                            getattr(e, "response", None)) from e
            except Exception:
                # Another error occurred, but we'll still create the stream
                # Just don't validate it was successful
                pass

        # Store in cache
        self._streams_cache[stream_id] = stream

        return stream

    def get(self, stream_id: str) -> DataStream:
        """Alias for from_id."""
        return self.from_id(stream_id)

    async def aget(self, stream_id: str) -> DataStream:
        """Alias for afrom_id."""
        return await self.afrom_id(stream_id)
