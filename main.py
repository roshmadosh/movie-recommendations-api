from fastapi import FastAPI
from model_assets.model import get_top_n
from pydantic import BaseModel

app = FastAPI()

class RequestObject(BaseModel):
    titles: list[str]
    count: int

@app.get('/')
async def root():
    return "Smoketest for my fastapi app."

@app.post('/recommendations')
async def predict(req: RequestObject):
    top_n = get_top_n(req.titles, req.count)
    return top_n



