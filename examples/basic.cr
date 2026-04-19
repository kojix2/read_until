require "../src/read_until"

manager = Minknow::Manager.new(
  Minknow::ConnectionConfig.new(host: "localhost", port: 9501, tls: true)
)

position = manager.register_position(
  Minknow::FlowCellPosition.new("demo-position", "localhost", 9600)
)

connection = manager.connect(position)
client = ReadUntil::ReadUntilClient.new(
  connection: connection,
  setup: ReadUntil::StreamSetup.new(first_channel: 1, last_channel: 128, one_chunk: true)
)

client.start
client.ingest(ReadUntil::ReadChunk.new(channel: 12, read_id: "read-0001", number: 1))
client.enqueue_unblock(12, "read-0001")

puts "endpoint=#{client.endpoint}"
puts "running=#{client.running?}"
puts "latest_read_id=#{client.latest_read(12).try(&.read_id)}"
puts "queued_actions=#{client.drain_actions.size}"

client.stop
