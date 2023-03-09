from fastapi import FastAPI

app = FastAPI()

@app.get('/')
async def root():
    return "Smoketest for my fastapi app."

@app.post('/predict')
async def predict(title: str):
    return f"The title of your query param is {title}"
