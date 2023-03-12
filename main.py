from fastapi import FastAPI
from model_assets.model import get_top_n

app = FastAPI()

@app.get('/')
async def root():
    return "Smoketest for my fastapi app."

@app.get('/recommendations')
async def predict(title: str, count: int):
    top_n = get_top_n(title, count)
    print(top_n)
    return {"success": True, "results": top_n}



