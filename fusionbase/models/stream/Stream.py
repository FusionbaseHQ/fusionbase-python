from pydantic import BaseModel
from typing import List

from typing import Optional
import httpx
import msgpack

from rich.progress import Progress
import pandas as pd
import asyncio


class Stream(BaseModel):
    key: str

    keywords: list = []
    unique_label: str | None = None
    data_items_unique: list | None = None
    #data_item_collections: list = []
    data_sample: dict | None = None
    source: dict | None = None
    store: dict | None = None
    store_version: str | None = None
    data_version: str | None = None
    data_updated_at: str | None = None
    #data: list | None = None
    unique_label: str | None = None
    meta: dict | None = None
    
    client: Optional[httpx.Client] = None
    aclient: Optional[httpx.AsyncClient] = None
    
    
    class Config:
        arbitrary_types_allowed = True
       
    
    
    def get_data(self, skip=0, limit=10, query: dict = None, version_boundary: str = None):    
        params = {k: v for k, v in {"skip": skip, "limit": limit, "query": query, "version_boundary": version_boundary}.items() if v is not None}    
        response = self.client.get(f"stream/data/{self.key}", params=params) 
        data = msgpack.unpackb(response.content)        
        return data
    
    
    def as_dataframe(self, log=True):
        return asyncio.get_event_loop().run_until_complete(self.async_as_dataframe(log))
    
    
    async def aget_data(self, skip=0, limit=10, query: dict = None, version_boundary: str = None):    
        params = {k: v for k, v in {"skip": skip, "limit": limit, "query": query, "version_boundary": version_boundary}.items() if v is not None}       
        response = await self.aclient.get(f"stream/data/{self.key}", timeout=60, params=params) 
        data = msgpack.unpackb(response.content)        
        return data
    
    
    async def async_as_dataframe(self, log=True):
        data_chunks = []
        semaphore = asyncio.Semaphore(5)  # Semaphore to limit concurrent requests

        async def fetch_data(skip):
            async with semaphore:
                data_chunk = await self.aget_data(skip=skip, limit=limit)
                return pd.DataFrame(data_chunk)

        limit = 20000  # Adjust this based on your requirements
        total_tasks = (self.meta.get('entry_count', 0) + limit - 1) // limit
        tasks = [fetch_data(skip) for skip in range(0, self.meta.get('entry_count', 0), limit)]

        # Use Rich's Progress context manager
        if log:
            with Progress() as progress:
                # Add a task to the progress bar (initially set to 0 of total_tasks)
                task_id = progress.add_task("[green]Downloading...", total=total_tasks)

                # Process tasks as they are completed
                for task in asyncio.as_completed(tasks):
                    data_chunk = await task
                    data_chunks.append(data_chunk)
                    # Update the progress bar
                    progress.update(task_id, advance=1)
        else:
            for task in asyncio.as_completed(tasks):
                data_chunk = await task
                data_chunks.append(data_chunk)
         

        full_data = pd.concat(data_chunks, ignore_index=True)
        full_data.sort_values(by=['fb_datetime'], ascending=[False], inplace=True)
        return full_data

    
    
    async def async_update(self, data, update_type="DATA", parameters=None):
        # Serialize your payload using msgpack
        payload = {
            "stream_key": self.key,
            "update_type": update_type,
            "parameters": parameters,
            "data": data
        }
        msgpack_payload = msgpack.packb(payload)

        # Send the serialized payload as the request body
        # Note: Set the 'Content-Type' to 'application/x-msgpack'
        headers = {'Content-Type': 'application/x-msgpack'}
        response = await self.aclient.post(
            f"stream/update-data", 
            content=msgpack_payload, 
            headers=headers,
            timeout=60
        ) 

        return response.json()
    
    
    async def asnyc_replace_data(self, data):
        pass
