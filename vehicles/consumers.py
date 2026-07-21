from channels.generic.websocket import AsyncJsonWebsocketConsumer


# firehose
class VehicleLocationConsumer(AsyncJsonWebsocketConsumer):
    @property
    def vehicle_id(self):
        return self.scope["url_route"]["kwargs"].get("vehicle_id")

    @property
    def group(self):
        if self.vehicle_id:
            return f"vehicle_location{self.vehicle_id}"
        return "vehicle_locations"

    async def connect(self):
        await self.channel_layer.group_add(self.group, self.channel_name)
        await self.accept()

        if self.vehicle_id:
            key = f"vehicle{self.vehicle_id}"
            connection = self.channel_layer.connection(
                self.channel_layer.consistent_hash(key)
            )
            item = await connection.get(key)
            if item:
                await self.send(text_data=f'{{"items": [{item.decode()}]}}')

    async def disconnect(self, close_code):
        print(close_code)
        await self.channel_layer.group_discard(self.group, self.channel_name)

    async def move_vehicles(self, event):
        await self.send_json({"items": event["items"]})
