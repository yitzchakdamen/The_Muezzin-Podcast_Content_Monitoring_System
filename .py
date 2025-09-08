from pymongo import MongoClient
from faster_whisper import WhisperModel
model = WhisperModel("base", device="cpu")

client=MongoClient()
client = MongoClient("mongodb://localhost:27017/")
mydb = client["podcast_files"]
mycollection = mydb["audio_files"]


from gridfs import GridFS


id = "3a046ce7db51681d2e46117e51653e6cdbe4709786f23eb929c8a72a21a7bb48"

fs = GridFS(database=mydb, collection="audio_files")
print("----")
id_s = fs.find_one({"file_id":id})

a = fs.get(id_s._id)
for file in fs.find():
    print("-----------")



import io

audio_stream = io.BytesIO(a.read())


segments, info = model.transcribe(audio_stream, language='en')
print("".join([segment.text for segment in segments ]))