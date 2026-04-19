require "./spec_helper"

describe ReadUntil do
  it "can be initialized with a Minknow connection" do
    config = Minknow::ConnectionConfig.new(host: "localhost", port: 9501)
    manager = Minknow::Manager.new(config)
    position = Minknow::FlowCellPosition.new("X4", "localhost", 9600)
    connection = manager.connect(position)

    client = ReadUntil::ReadUntilClient.new(connection: connection)

    client.connection.should eq(connection)
    client.endpoint.should eq("localhost:9600")
  end

  it "tracks running state" do
    client = ReadUntil::ReadUntilClient.new

    client.running?.should be_false
    client.start
    client.running?.should be_true
    client.stop
    client.running?.should be_false
  end

  it "stores the latest read by channel" do
    client = ReadUntil::ReadUntilClient.new
    older = ReadUntil::ReadChunk.new(channel: 7, read_id: "read-a", number: 1)
    newer = ReadUntil::ReadChunk.new(channel: 7, read_id: "read-b", number: 2)

    client.ingest(older)
    client.ingest(newer)

    client.latest_read(7).should eq(newer)
  end

  it "queues actions and drains them in order" do
    client = ReadUntil::ReadUntilClient.new

    first = client.enqueue_unblock(3, "read-1")
    second = client.enqueue_stop_receiving(4, "read-2")

    client.drain_actions.should eq([first, second])
    client.drain_actions.should be_empty
  end

  it "has a version number" do
    ReadUntil::VERSION.should be_a(String)
  end
end
