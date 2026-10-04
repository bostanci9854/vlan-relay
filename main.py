import asyncio
import websockets
import os
import json

PORT = int(os.environ.get("PORT", 8765))

# ROOMS = { room_id: { websocket: {"name": str, "role": "HOST"|"CLIENT"} } }
ROOMS = {}

async def notify_host(room_id):
    """Sadece Host olan kullanıcıya güncel cihaz listesini gönderir."""
    if room_id in ROOMS:
        clients = [info["name"] for info in ROOMS[room_id].values()]
        msg = json.dumps({"type": "USER_LIST", "users": clients})
        
        for ws, info in ROOMS[room_id].items():
            if info["role"] == "HOST":
                try:
                    await ws.send(msg)
                except Exception:
                    pass

async def handler(websocket):
    current_room = None
    is_host = False
    try:
        async for message in websocket:
            # Odaya kayıt istemi: "JOIN:ODA_ID:CLIENT_ID:ROLE"
            if isinstance(message, str) and message.startswith("JOIN:"):
                parts = message.split(":")
                current_room = parts[1]
                client_id = parts[2] if len(parts) > 2 else "Bilinmeyen Cihaz"
                role = parts[3] if len(parts) > 3 else "CLIENT"

                # CLIENT Katılmaya Çalışıyorsa ve Oda Yoksa Katılımı Reddet
                if role == "CLIENT" and current_room not in ROOMS:
                    await websocket.send(json.dumps({"type": "ERROR", "message": "Oda bulunamadı veya kapatılmış."}))
                    await websocket.close()
                    return

                if current_room not in ROOMS:
                    ROOMS[current_room] = {}
                
                if role == "HOST":
                    is_host = True

                ROOMS[current_room][websocket] = {"name": client_id, "role": role}
                await websocket.send(json.dumps({"type": "SUCCESS", "message": "Odaya katılım başarılı."}))
                await notify_host(current_room)

            # Şifreli paket iletimi (Binary)
            elif isinstance(message, bytes) and current_room in ROOMS:
                for ws in ROOMS[current_room].keys():
                    if ws != websocket:
                        try:
                            await ws.send(message)
                        except Exception:
                            pass

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if current_room and current_room in ROOMS:
            if websocket in ROOMS[current_room]:
                del ROOMS[current_room][websocket]
            
            # Eğer ayrılan kişi HOST ise odayı tamamen kapat ve Client'ları bilgilendir
            if is_host:
                for ws in list(ROOMS[current_room].keys()):
                    try:
                        await ws.send(json.dumps({"type": "ERROR", "message": "Host odayı kapattı."}))
                        await ws.close()
                    except Exception:
                        pass
                if current_room in ROOMS:
                    del ROOMS[current_room]
            else:
                if len(ROOMS[current_room]) == 0:
                    del ROOMS[current_room]
                else:
                    await notify_host(current_room)

async def main():
    async with websockets.serve(handler, "0.0.0.0", PORT):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
