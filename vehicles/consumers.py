from channels.generic.websocket import AsyncJsonWebsocketConsumer


# firehose
class VehicleLocationConsumer(AsyncJsonWebsocketConsumer):
    @property
    def group(self):
        vehicle_id = self.scope["url_route"]["kwargs"].get("vehicle_id")
        if vehicle_id:
            return f"vehicle_location{vehicle_id}"
        return "vehicle_locations"

    async def connect(self):
        await self.channel_layer.group_add(self.group, self.channel_name)
        await self.accept()

    async def disconnect(self, close_code):
        print(close_code)
        await self.channel_layer.group_discard(self.group, self.channel_name)

    async def move_vehicles(self, event):
        await self.send_json({"items": event["items"]})
