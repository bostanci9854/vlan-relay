import asyncio
import websockets
import os

# Render PORT çevre değişkenini otomatik atar
PORT = int(os.environ.get("PORT", 8765))

ROOMS = {}  # {room_id: [websocket_client1, websocket_client2]}

async def handler(websocket):
    try:
        async for message in websocket:
            if isinstance(message, str) and message.startswith("JOIN:"):
                room_id = message.split(":")[1]
                if room_id not in ROOMS:
                    ROOMS[room_id] = []
                ROOMS[room_id].append(websocket)
                websocket.room_id = room_id
                print(f"[+] İstemci {room_id} odasına katıldı. Odadaki kişi sayısı: {len(ROOMS[room_id])}")
            else:
                room_id = getattr(websocket, 'room_id', None)
                if room_id and room_id in ROOMS:
                    for client in ROOMS[room_id]:
                        if client != websocket:
                            await client.send(message)
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        room_id = getattr(websocket, 'room_id', None)
        if room_id and room_id in ROOMS:
            if websocket in ROOMS[room_id]:
                ROOMS[room_id].remove(websocket)
            if len(ROOMS[room_id]) == 0:
                del ROOMS[room_id]

async def main():
    print(f"[+] Render Relay Sunucusu {PORT} portunda başlatılıyor...")
    async with websockets.serve(handler, "0.0.0.0", PORT):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())