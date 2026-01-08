from fastapi import FastAPI
from fastapi.responses import FileResponse
import uvicorn
import os

app = FastAPI()

HTML_FILE = os.path.join(os.path.dirname(__file__), "index.html")

@app.get("/")
def index():
    return FileResponse(HTML_FILE)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)
