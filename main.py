from typing import List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
# 導入Request上下文對象，用來在前後台之間傳遞參數
from starlette.requests import Request
# 實例化一個模板引擎對象，指定模板所在路徑
from fastapi.templating import Jinja2Templates
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

import asyncio

app = FastAPI()
templates = Jinja2Templates(directory="./templates")

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "chat-topic"
KAFKA_CONSUMER_GROUP = "chat-topic-group"
loop = asyncio.get_event_loop()


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def produce(self, message: str, websocket: WebSocket,):
        self.producer = AIOKafkaProducer(
            loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        await self.producer.start()
        try:
            value_json = message.encode('utf-8')
            await self.producer.send_and_wait(topic=KAFKA_TOPIC, value=value_json)
        finally:
            await self.producer.stop()

    async def consume(self):
        self.consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            loop=loop,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_CONSUMER_GROUP
        )
        await self.consumer.start()
        try:
            async for msg in self.consumer:
                return (msg.value).decode("utf-8")
        finally:
            await self.consumer.stop()


manager = ConnectionManager()


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse('index.html', {"request": request})


@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: int):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            await manager.send_personal_message(f"You wrote: {data}", websocket)
            await manager.produce(data, websocket)
            data_kafka = await manager.consume()
            await manager.broadcast(f"Client #{client_id} says: {data_kafka}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        await manager.broadcast(f"Client #{client_id} left the chat")
