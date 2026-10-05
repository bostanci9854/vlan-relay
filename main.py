import asyncio
import json
import os
import re
import websockets


PORT = int(os.environ.get("PORT", 8765))

ROOMS = {}

ROOM_CODE_PATTERN = re.compile(r"^[A-Z0-9]{8}$")

MAX_CLIENTS_PER_ROOM = 2


async def send_json(websocket, data):
    try:
        await websocket.send(
            json.dumps(data)
        )
    except Exception:
        pass


async def broadcast_user_list(room_id):

    room = ROOMS.get(room_id)

    if not room:
        return

    users = []

    for user in room:

        users.append({
            "device_name": user["name"],
            "role": user["role"],
            "ip": user["ip"]
        })

    message = json.dumps({
        "type": "USER_LIST",
        "users": users
    })

    disconnected = []

    for user in room:

        try:

            await user["ws"].send(
                message
            )

        except Exception:

            disconnected.append(user)

    for user in disconnected:

        if user in room:
            room.remove(user)


async def handler(websocket):

    current_room = None
    current_user = None

    try:

        async for message in websocket:

            # =================================================
            # JOIN
            # =================================================

            if (
                isinstance(message, str)
                and message.startswith("JOIN:")
            ):

                parts = message.split(":", 3)

                if len(parts) != 4:

                    await send_json(
                        websocket,
                        {
                            "type": "ERROR",
                            "message": "Geçersiz bağlantı isteği."
                        }
                    )

                    continue

                _, room_code, device_name, role = parts

                room_code = room_code.strip().upper()
                device_name = device_name.strip()[:64]
                role = role.strip().upper()

                # ---------------------------------------------
                # ODA KODU
                # ---------------------------------------------

                if not ROOM_CODE_PATTERN.fullmatch(
                    room_code
                ):

                    await send_json(
                        websocket,
                        {
                            "type": "ERROR",
                            "message": "Geçersiz oda kodu."
                        }
                    )

                    continue

                # ---------------------------------------------
                # ROLE
                # ---------------------------------------------

                if role not in ("HOST", "CLIENT"):

                    await send_json(
                        websocket,
                        {
                            "type": "ERROR",
                            "message": "Geçersiz bağlantı türü."
                        }
                    )

                    continue

                # ---------------------------------------------
                # AYNI BAĞLANTI TEKRAR JOIN YAPAMAZ
                # ---------------------------------------------

                if current_room is not None:

                    await send_json(
                        websocket,
                        {
                            "type": "ERROR",
                            "message": "Bu bağlantı zaten bir odaya bağlı."
                        }
                    )

                    continue

                # ---------------------------------------------
                # ODAYI OLUŞTUR
                # ---------------------------------------------

                if room_code not in ROOMS:

                    ROOMS[room_code] = []

                room = ROOMS[room_code]

                # ---------------------------------------------
                # ODA DOLU MU?
                # ---------------------------------------------

                if len(room) >= MAX_CLIENTS_PER_ROOM:

                    await send_json(
                        websocket,
                        {
                            "type": "ERROR",
                            "message": "Bu oda dolu."
                        }
                    )

                    continue

                # ---------------------------------------------
                # HOST
                # ---------------------------------------------

                if role == "HOST":

                    host_exists = any(
                        user["role"] == "HOST"
                        for user in room
                    )

                    if host_exists:

                        await send_json(
                            websocket,
                            {
                                "type": "ERROR",
                                "message": "Bu odada zaten bir Host bulunuyor."
                            }
                        )

                        continue

                    assigned_ip = "10.8.0.1"

                # ---------------------------------------------
                # CLIENT
                # ---------------------------------------------

                else:

                    client_exists = any(
                        user["role"] == "CLIENT"
                        for user in room
                    )

                    if client_exists:

                        await send_json(
                            websocket,
                            {
                                "type": "ERROR",
                                "message": "Bu odada zaten bir Client bulunuyor."
                            }
                        )

                        continue

                    assigned_ip = "10.8.0.2"

                # ---------------------------------------------
                # USER
                # ---------------------------------------------

                current_user = {
                    "ws": websocket,
                    "name": device_name,
                    "role": role,
                    "ip": assigned_ip
                }

                room.append(
                    current_user
                )

                current_room = room_code

                print(
                    f"[+] {device_name} "
                    f"({role}) "
                    f"-> {room_code} "
                    f"[{assigned_ip}] "
                    f"({len(room)}/{MAX_CLIENTS_PER_ROOM})"
                )

                # ---------------------------------------------
                # SUCCESS
                # ---------------------------------------------

                await send_json(
                    websocket,
                    {
                        "type": "SUCCESS",
                        "room": room_code,
                        "role": role,
                        "ip": assigned_ip
                    }
                )

                # ---------------------------------------------
                # USER LIST
                # ---------------------------------------------

                await broadcast_user_list(
                    room_code
                )

                continue

            # =================================================
            # BINARY IP PACKET
            # =================================================

            if isinstance(message, bytes):

                if not current_room:
                    continue

                room = ROOMS.get(
                    current_room
                )

                if not room:
                    continue

                # Paketi odadaki diğer kullanıcıya gönder.
                for user in room:

                    if user["ws"] == websocket:
                        continue

                    try:

                        await user["ws"].send(
                            message
                        )

                    except Exception as e:

                        print(
                            f"[!] Paket gönderilemedi: {e}"
                        )

    except websockets.exceptions.ConnectionClosed:

        pass

    except Exception as e:

        print(
            f"[!] Handler hatası: {e}"
        )

    finally:

        if (
            current_room
            and current_room in ROOMS
        ):

            room = ROOMS[current_room]

            if current_user in room:

                room.remove(
                    current_user
                )

            print(
                f"[-] İstemci ayrıldı: "
                f"{current_user['name'] if current_user else 'Bilinmeyen'} "
                f"({current_room})"
            )

            if len(room) == 0:

                del ROOMS[current_room]

            else:

                await broadcast_user_list(
                    current_room
                )


async def main():

    print(
        f"[+] VLAN Connect Relay "
        f"{PORT} portunda başlatılıyor..."
    )

    async with websockets.serve(
        handler,
        "0.0.0.0",
        PORT,
        max_size=10 * 1024 * 1024,
        ping_interval=20,
        ping_timeout=20
    ):

        await asyncio.Future()


if __name__ == "__main__":

    asyncio.run(
        main()
    )
