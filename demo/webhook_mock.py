"""Webhook receiver for the demo: /ok accepts, /fail always answers 500."""

from fastapi import FastAPI, Request, Response

app = FastAPI(title="Webhook mock")


@app.post("/ok")
async def ok(request: Request) -> dict[str, bool]:
    print("webhook received:", await request.json(), flush=True)
    return {"ok": True}


@app.post("/fail")
async def fail(request: Request) -> Response:
    print("webhook rejected:", await request.json(), flush=True)
    return Response(status_code=500)
