import time
import json
import logging
import aiohttp
import csrmesh
import asyncio
import paho.mqtt.client as mqtt
import asyncio_mqtt

from importlib import metadata
from typing import Iterable, List
from bleak import BleakClient, BleakScanner
from bleak.exc import BleakError

MQTT_BROKER = "hass.local"
MQTT_USER = ""
MQTT_PASSWORD = ""

AVION_HOST = "https://api.avi-on.com"
AVION_USER = ""
AVION_PASSWORD = ""

HALO_PRODUCT_IDS = (162,93,)
BLUETOOTH_TIMEOUT = 20

devices = []

class HaloHomeError(Exception):
    pass

def _format_mac_address(mac_address: str) -> str:
    iterator = iter(mac_address.lower())
    pairs = zip(iterator, iterator)
    return ":".join(a + b for a, b in pairs)


class Device:
    def __init__(
        self,
        location: "LocationConnection",
        device_id: int,
        device_name: str,
        pid: str,
        avid: str,
        product_id: str,
        mac_address: str,
    ):
        self.location = location
        self.device_id = device_id
        self.device_name = device_name
        self.pid = pid
        self.avid = avid
        self.product_id = product_id
        self.mac_address = mac_address
        self.state = True
        self.brightness = 20
        self.color = 153

    async def set_state(self, state: bool) -> bool:
        self.state = state
        if self.state:
            return await self.location.set_brightness(self.device_id, self.brightness)
        else:
            return await self.location.set_brightness(self.device_id, 0)

    async def set_brightness(self, brightness: int) -> bool:
        self.brightness = brightness
        if self.state:
            return await self.location.set_brightness(self.device_id, self.brightness)
        else:
            return await self.location.set_brightness(self.device_id, 0)

    async def set_color_temp(self, color: int) -> bool:
        self.color = color
        return await self.location.set_color_temp(self.device_id, color)

    def __str__(self):
        return f"Device<{self.device_name} ({self.device_id}), {self.mac_address}>"

    def __repr__(self):
        return str(self)


class LocationConnection:
    CHARACTERISTIC_LOW = "c4edc000-9daf-11e3-8003-00025b000b00"
    CHARACTERISTIC_HIGH = "c4edc000-9daf-11e3-8004-00025b000b00"
    CHARACTERISTIC_READ = "c4edc000-9daf-11e3-8005-00025b000b00"

    def __init__(self, location_id: str, passphrase: str, devices: List[dict], timeout: int = BLUETOOTH_TIMEOUT):
        self.devices = []
        self.mesh_connection = None
        self.location_id = location_id
        self.key = csrmesh.crypto.generate_key(passphrase.encode("ascii") + b"\x00\x4d\x43\x50")
        self.timeout = timeout

        for raw_device in devices:
            device_id = raw_device["device_id"]
            device_name = raw_device["device_name"]
            pid = raw_device["pid"]
            avid = raw_device["avid"]
            product_id = raw_device["product_id"]
            mac_address = raw_device["mac_address"]
            device = Device(self, device_id, device_name, pid, avid, product_id, mac_address)
            self.devices.append(device)

    async def _priority_devices(self):
        scanned_devices = sorted(await BleakScanner.discover(), key=lambda d: d.rssi)
        sorted_addresses = [d.address.lower() for d in scanned_devices]

        def priority(device: Device):
            try:
                return sorted_addresses.index(device.mac_address)
            except ValueError:
                return -1

        return sorted(self.devices, key=priority, reverse=True)

    async def disconnected_callback():
        print("disconnected_callback!")
        self.mesh_connection = None

    async def _connect(self):
        for device in await self._priority_devices():
            try:
                client = BleakClient(device.mac_address, timeout=self.timeout)
                await client.connect()
                self.mesh_connection = client
                return
            except BleakError as e:
                pass

    async def _send_packet(self, packet: bytes) -> bool:
        csrpacket = csrmesh.crypto.make_packet(self.key, csrmesh.crypto.random_seq(), packet)
        low = csrpacket[:20]
        high = csrpacket[20:]

        for _ in range(3):
            try:
                print ("_send_packet")
                print (self)
                print (self.mesh_connection)
                if self.mesh_connection is None:
                    await self._connect()

                await self.mesh_connection.write_gatt_char(self.CHARACTERISTIC_LOW, low)
                await self.mesh_connection.write_gatt_char(self.CHARACTERISTIC_HIGH, high)

                return True
            except Exception as e:
                print(e);
                self.mesh_connection = None

        return False

    async def set_brightness(self, device_id: int, brightness: int) -> bool:
        packet = bytes([0x80 + device_id, 0x80, 0x73, 0, 0x0A, 0, 0, 0, brightness, 0, 0, 0, 0])
        print("set_brightness {} {}".format(device_id, brightness))
        return await self._send_packet(packet)

    async def set_color_temp(self, device_id: int, color: int) -> bool:
        color_bytes = bytearray(color.to_bytes(2, byteorder="big"))
        packet = bytes([0x80 + device_id, 0x80, 0x73, 0, 0x1D, 0, 0, 0, 0x01, *color_bytes, 0, 0])
        return await self._send_packet(packet)


class Connection:
    def __init__(self, location_devices: List[dict], timeout: int = BLUETOOTH_TIMEOUT):
        self.devices = []
        self.timeout = timeout

        for raw_location in location_devices:
            location = LocationConnection(**raw_location, timeout = timeout)
            self.devices.extend(location.devices)


async def _make_request(
    host: str,
    path: str,
    body: dict = None,
    auth_token: str = None,
    timeout: int = BLUETOOTH_TIMEOUT,
):
    method = "GET" if body is None else "POST"
    url = host + path

    headers = {}
    if auth_token:
        headers["Accept"] = "application/api.avi-on.v2"
        headers["Authorization"] = f"Token {auth_token}"

    async with aiohttp.ClientSession() as session:
        async with session.request(method, url, json=body, headers=headers, timeout=timeout) as response:
            return await response.json()


async def _load_devices(
    host: str, auth_token: str, location_id: str, product_ids: Iterable[int], timeout: int
) -> List[dict]:
    response = await _make_request(
        host, f"locations/{location_id}/abstract_devices", auth_token=auth_token, timeout=timeout
    )
    raw_devices = response["abstract_devices"]
    devices = []

    device_id_offset = None
    for raw_device in raw_devices:
        if raw_device["product_id"] not in product_ids:
            continue

        device_id_offset = device_id_offset or raw_device["avid"]

        device_id = raw_device["avid"] - device_id_offset
        pid = raw_device["pid"]
        device_name = raw_device["name"]
        avid = raw_device["avid"]
        product_id = raw_device["product_id"]
        mac_address = _format_mac_address(raw_device["friendly_mac_address"])
        device = {"device_id": device_id, "pid": pid, "device_name": device_name, "avid": avid, "product_id": product_id, "mac_address": mac_address}
        devices.append(device)

    return devices


async def _load_locations(host: str, auth_token: str, product_ids: Iterable[int], timeout: int) -> List[dict]:
    response = await _make_request(host, "locations", auth_token=auth_token, timeout=timeout)
    locations = []
    for raw_location in response["locations"]:
        location_id = str(raw_location["id"])
        devices = await _load_devices(host, auth_token, location_id, product_ids, timeout)
        location = {"location_id": location_id, "passphrase": raw_location["passphrase"], "devices": devices}
        locations.append(location)

    return locations


async def list_devices(
    email: str,
    password: str,
    host: str = AVION_HOST,
    product_ids: Iterable[int] = HALO_PRODUCT_IDS,
    timeout: int = BLUETOOTH_TIMEOUT):

    if not host.endswith("/"):
        host += "/"

    login_body = {"email": email, "password": password}
    response = await _make_request(host, "sessions", login_body, timeout=timeout)
    if "credentials" not in response:
        raise HaloHomeError("Invalid credentials for HALO Home")
    auth_token = response["credentials"]["auth_token"]

    return await _load_locations(host, auth_token, product_ids, timeout)

async def publish_state(client, device):
    if device.state:
        state_str = "ON"
    else:
        state_str = "OFF"
    state_payload = {
        "state" : state_str,
        "brightness" : device.brightness,
        "color_temp" : device.color
    }
    light_name = "halo_{}_{}".format(device.product_id,device.avid)
    state_topic = "{}/state".format(light_name)
    print(state_topic)
    print(json.dumps(state_payload, indent = 4))
    await client.publish(state_topic, json.dumps(state_payload), qos = 0, retain = True)

async def connect(client):

    while True:
        try:
            locations = await list_devices(AVION_USER, AVION_PASSWORD)
            connection = Connection(locations)
            break
        except (HaloHomeError, OSError, aiohttp.ClientError) as exception:
            time.sleep(180)

    time.sleep(1)

    for device in connection.devices:
        devices.append(device)

        light_name = "halo_{}_{}".format(device.product_id,device.avid)

        command_topic = "{}/set".format(light_name)
        await client.subscribe(command_topic)

        config_topic = "homeassistant/light/{}/config".format(light_name)
        config_payload = {
            "name" : device.device_name,
            "platform" : "mqtt",
            "schema" : "json",
            "unique_id" : "halo_{}_{}".format(device.product_id,device.avid),
            "state_topic" : "{}/state".format(light_name),
            "command_topic" : command_topic,
            "brightness": True,
            "color_mode": True,
            "supported_color_modes": ["brightness", "color_temp", "onoff"]
        }

        await client.publish(config_topic, json.dumps(config_payload), qos = 1, retain = False)

        time.sleep(1)

        await publish_state(client, device)

        time.sleep(1)

async def handle(message, client):
    if not str(message.topic).startswith("halo_"):
        return
    if not str(message.topic).endswith("set"):
        return
    avid = str(message.topic).split("/")[0].split("_")[2]
    for device in devices:
        if str(avid) == str(device.avid):
            settings = json.loads(message.payload)
            if "brightness" in settings:
                device.brightness = int(settings["brightness"])
                device.state = True
                await publish_state(client, device)
                await device.set_brightness(int(settings["brightness"]))
            elif "state" in settings:
                state = settings["state"]
                if state == "ON":
                    device.state = True
                    await publish_state(client, device)
                    await device.set_state(True)            
                if state == "OFF":
                    device.state = False
                    await publish_state(client, device)
                    await device.set_state(False)
            await publish_state(client, device)
            break

async def main():
    async with asyncio_mqtt.Client(
        hostname=MQTT_BROKER,
        port=1883,
        username=MQTT_USER,
        password=MQTT_PASSWORD
    ) as client:
        await connect(client)
        async with client.messages() as messages:
            await client.subscribe("#")
            async for message in messages:
                await handle(message, client)

asyncio.run(main())
