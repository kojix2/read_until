require "../src/read_until"

# Demonstrates constructing a Client and Session with the new API.

config = Minknow::ConnectionConfig.new(host: "localhost", port: 9501, tls: true)
manager = Minknow::Manager.new(config)
position = Minknow::FlowCellPosition.new("demo-position", "localhost", 9600)
connection = manager.connect(position)

client = ReadUntil::Client.new(connection)
session = client.new_session(
  channels: 1..128,
  mode: ReadUntil::StreamMode::OneChunk,
  prefilter: ReadUntil::Prefilter.strand_like(:strand, :adapter),
)

# Seed the session buffer with a synthetic read to show pull APIs.
session.buffer.push(ReadUntil::Read.new(
  channel: 12,
  id: "read-0001",
  chunk_start_sample: 0_u64,
  raw_bytes: Bytes[1, 0, 2, 0],
))

puts "endpoint=#{client.endpoint}"
puts "running=#{session.running?}"
puts "queue_length=#{session.buffer.size}"

result = session.pop_reads(max: 1)
puts "popped_channel=#{result[0].channel}"
puts "popped_read_id=#{result[0].id}"
puts "samples=#{result[0].signal(Int16).to_a.inspect}"
