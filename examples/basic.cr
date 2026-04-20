require "../src/read_until"

# Demonstrates constructing a ReadUntilClient and inspecting its properties
# without opening a live MinKNOW connection (no actual gRPC call is made here).
#
# To run against a real MinKNOW instance, set environment variables and call
# client.run { ... } to start the bidirectional stream.

config = Minknow::ConnectionConfig.new(host: "localhost", port: 9501, tls: true)
manager = Minknow::Manager.new(config)
position = Minknow::FlowCellPosition.new("demo-position", "localhost", 9600)
connection = manager.connect(position)

options = ReadUntil::StreamOptions.new(
  first_channel: 1,
  last_channel: 128,
  one_chunk: true,
)

client = ReadUntil::ReadUntilClient.new(connection: connection, options: options)

# Seed the cache with a test chunk to show the API surface.
client.read_cache[12] = ReadUntil::ReadChunk.new(
  channel: 12,
  id: "read-0001",
  chunk_start_sample: 0_u64,
  raw_data: Bytes[1, 2, 3, 4],
)

puts "endpoint=#{client.endpoint}"
puts "running=#{client.running?}"
puts "queue_length=#{client.queue_length}"

result = client.read_chunks(1)
puts "popped_channel=#{result[0][0]}"
puts "popped_read_id=#{result[0][1].id}"
