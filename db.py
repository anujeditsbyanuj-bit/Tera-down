from motor.motor_asyncio import AsyncIOMotorClient

MONGO_DB_URI = "mongodb+srv://Anujedit:Anujedit@cluster0.7cs2nhd.mongodb.net/?appName=Cluster0"

_client = AsyncIOMotorClient(MONGO_DB_URI)
db = _client["terabox_bot"]
