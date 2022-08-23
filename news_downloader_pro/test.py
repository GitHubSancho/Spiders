import asyncio
from pprint import pprint
import motor.motor_asyncio


class Client:
    def __init__(self) -> None:

        self.client = motor.motor_asyncio.AsyncIOMotorClient(
            'mongodb://localhost:27017')
        self.db = self.client['news_downloader_pro']
        self.coll = self.db['test110']

    async def do_find_one(self):
        document = await self.coll.find({})
        return document

    def dothis(self):
        a = self.do_find_one()
        pprint(a)


# loop = client.get_io_loop()
client = Client()
loop = asyncio.get_event_loop()
loop.run_until_complete(client.dothis())