import asyncio
import json
import os
import re
import websockets


PORT = int(os.environ.get("PORT", 8765))

ROOMS = {}

ROOM_CODE_PATTERN = re.compile(r"^[A-Z0-9]{8}$")

MAX_CLIENTS_PER_ROOM = 2


# =========================================================
# JSON GÖNDER
# =========================================================

async def send_json(websocket, data):

    try:

        await websocket.send(
            json.dumps(data)
        )

    except Exception:
        pass


# =========================================================
# ODA KULLANICILARINI BİLDİR
# =========================================================

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

            disconnected.append(
                user
            )

    for user in disconnected:

        if user in room:

            room.remove(
                user
            )


# =========================================================
# ODAYI KAPAT
# =========================================================

async def close_room(room_code):

    room = ROOMS.get(room_code)

    if not room:
        return

    print(
        f"[*] Oda kapatılıyor: {room_code}"
    )

    # Odadaki kullanıcılara bildir
    for user in list(room):

        try:

            await send_json(
                user["ws"],
                {
                    "type": "ROOM_CLOSED",
                    "message": "Oda Host tarafından kapatıldı."
                }
            )

        except Exception:
            pass

    # Odayı tamamen sil
    ROOMS.pop(
        room_code,
        None
    )

    print(
        f"[-] Oda tamamen kapatıldı: {room_code}"
    )


# =========================================================
# CLIENT'I ODADAN ÇIKAR
# =========================================================

async def remove_user_from_room(
    room_code,
    user
):

    room = ROOMS.get(room_code)

    if not room:
        return

    if user in room:

        room.remove(
            user
        )

    print(
        f"[-] Kullanıcı odadan ayrıldı: "
        f"{user['name']} ({room_code})"
    )

    # Eğer ayrılan kişi Host ise
    # oda tamamen kapanır.
    if user["role"] == "HOST":

        await close_room(
            room_code
        )

        return

    # Client ayrıldıysa ve Host hala varsa
    if len(room) > 0:

        await broadcast_user_list(
            room_code
        )

    else:

        ROOMS.pop(
            room_code,
            None
        )


# =========================================================
# WEBSOCKET HANDLER
# =========================================================

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

                parts = message.split(
                    ":",
                    3
                )

                if len(parts) != 4:

                    await send_json(
                        websocket,
                        {
                            "type": "ERROR",
                            "message":
                                "Geçersiz bağlantı isteği."
                        }
                    )

                    continue

                _, room_code, device_name, role = parts

                room_code = (
                    room_code
                    .strip()
                    .upper()
                )

                device_name = (
                    device_name
                    .strip()
                    [:64]
                )

                role = (
                    role
                    .strip()
                    .upper()
                )

                # =================================================
                # KOD KONTROLÜ
                # =================================================

                if not ROOM_CODE_PATTERN.fullmatch(
                    room_code
                ):

                    await send_json(
                        websocket,
                        {
                            "type": "ERROR",
                            "message":
                                "Geçersiz oda kodu."
                        }
                    )

                    continue

                # =================================================
                # ROLE KONTROLÜ
                # =================================================

                if role not in (
                    "HOST",
                    "CLIENT"
                ):

                    await send_json(
                        websocket,
                        {
                            "type": "ERROR",
                            "message":
                                "Geçersiz bağlantı türü."
                        }
                    )

                    continue

                # =================================================
                # AYNI BAĞLANTI ZATEN ODADAYSA
                # =================================================

                if current_room is not None:

                    await send_json(
                        websocket,
                        {
                            "type": "ERROR",
                            "message":
                                "Bu bağlantı zaten bir odaya bağlı."
                        }
                    )

                    continue

                # =================================================
                # HOST
                # =================================================

                if role == "HOST":

                    # Host yeni oda oluşturabilir.
                    # Ancak aynı kod zaten kullanılıyorsa
                    # ikinci Host'a izin verme.

                    if room_code in ROOMS:

                        await send_json(
                            websocket,
                            {
                                "type": "ERROR",
                                "message":
                                    "Bu oda kodu zaten kullanımda."
                            }
                        )

                        continue

                    ROOMS[room_code] = []

                    room = ROOMS[
                        room_code
                    ]

                    assigned_ip = "10.8.0.1"

                # =================================================
                # CLIENT
                # =================================================

                else:

                    # -------------------------------------------------
                    # EN ÖNEMLİ KONTROL
                    #
                    # CLIENT olmayan bir odaya giremez.
                    # -------------------------------------------------

                    if room_code not in ROOMS:

                        print(
                            f"[!] Olmayan odaya "
                            f"Client bağlantı denemesi: "
                            f"{room_code}"
                        )

                        await send_json(
                            websocket,
                            {
                                "type": "ERROR",
                                "message":
                                    "Bu oda bulunamadı veya kapatılmış."
                            }
                        )

                        continue

                    room = ROOMS[
                        room_code
                    ]

                    # -------------------------------------------------
                    # Odada Host var mı?
                    # -------------------------------------------------

                    host_exists = any(
                        user["role"] == "HOST"
                        for user in room
                    )

                    if not host_exists:

                        await send_json(
                            websocket,
                            {
                                "type": "ERROR",
                                "message":
                                    "Bu odada aktif bir Host bulunmuyor."
                            }
                        )

                        continue

                    # -------------------------------------------------
                    # Client zaten var mı?
                    # -------------------------------------------------

                    client_exists = any(
                        user["role"] == "CLIENT"
                        for user in room
                    )

                    if client_exists:

                        await send_json(
                            websocket,
                            {
                                "type": "ERROR",
                                "message":
                                    "Bu odada zaten bir Client bulunuyor."
                            }
                        )

                        continue

                    assigned_ip = "10.8.0.2"

                # =================================================
                # ODAYA KULLANICI EKLE
                # =================================================

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
                    f"({role}) -> "
                    f"{room_code} "
                    f"[{assigned_ip}] "
                    f"({len(room)}/{MAX_CLIENTS_PER_ROOM})"
                )

                # =================================================
                # BAŞARILI
                # =================================================

                await send_json(
                    websocket,
                    {
                        "type": "SUCCESS",
                        "room": room_code,
                        "role": role,
                        "ip": assigned_ip
                    }
                )

                # Kullanıcı listesini gönder
                await broadcast_user_list(
                    room_code
                )

                continue

            # =================================================
            # LEAVE
            # =================================================

            if (
                isinstance(message, str)
                and message == "LEAVE"
            ):

                if current_room and current_user:

                    room_to_leave = current_room
                    user_to_leave = current_user

                    current_room = None
                    current_user = None

                    await remove_user_from_room(
                        room_to_leave,
                        user_to_leave
                    )

                continue

            # =================================================
            # BINARY PACKET
            # =================================================

            if isinstance(message, bytes):

                if not current_room:

                    continue

                room = ROOMS.get(
                    current_room
                )

                if not room:

                    continue

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

    # =========================================================
    # BAĞLANTI KAPANDI
    # =========================================================

    except websockets.exceptions.ConnectionClosed:

        pass

    except Exception as e:

        print(
            f"[!] Handler hatası: {e}"
        )

    # =========================================================
    # CLEANUP
    # =========================================================

    finally:

        if current_room and current_user:

            room_to_leave = current_room
            user_to_leave = current_user

            current_room = None
            current_user = None

            await remove_user_from_room(
                room_to_leave,
                user_to_leave
            )


# =========================================================
# SERVER
# =========================================================

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


# =========================================================
# START
# =========================================================

if __name__ == "__main__":

    asyncio.run(
        main()
    )
