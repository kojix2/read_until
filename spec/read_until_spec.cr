require "./spec_helper"

describe ReadUntil do
  it "has a version number" do
    ReadUntil::VERSION.should be_a(String)
  end
end

describe ReadUntil::ReadChunk do
  it "stores all fields" do
    chunk = ReadUntil::ReadChunk.new(
      channel: 3,
      id: "read-abc",
      chunk_start_sample: 1000_u64,
      chunk_length: 200_u64,
      chunk_classifications: [83, 83],
      raw_data: Bytes[1, 2, 3],
      median_before: 180.5_f32,
      median: 75.0_f32,
    )
    chunk.channel.should eq(3)
    chunk.id.should eq("read-abc")
    chunk.chunk_start_sample.should eq(1000_u64)
    chunk.chunk_classifications.should eq([83, 83])
    chunk.median_before.should eq(180.5_f32)
  end

  it "defaults optional fields" do
    chunk = ReadUntil::ReadChunk.new(channel: 1, id: "r1")
    chunk.raw_data.should eq(Bytes.empty)
    chunk.chunk_classifications.should be_empty
  end
end

describe ReadUntil::ReadCache do
  it "raises when size < 1" do
    expect_raises(ArgumentError) { ReadUntil::ReadCache.new(0) }
  end

  it "stores and retrieves the latest chunk per channel" do
    cache = ReadUntil::ReadCache.new(10)
    a = ReadUntil::ReadChunk.new(channel: 1, id: "r1")
    b = ReadUntil::ReadChunk.new(channel: 1, id: "r2")
    cache[1] = a
    cache[1] = b
    cache[1]?.try(&.id).should eq("r2")
  end

  it "counts replaced chunks from the same read" do
    cache = ReadUntil::ReadCache.new(10)
    r = ReadUntil::ReadChunk.new(channel: 1, id: "r1")
    cache[1] = r
    cache[1] = r
    cache.replaced.should eq(1)
    cache.missed.should eq(0)
  end

  it "counts missed reads when a distinct read overwrites" do
    cache = ReadUntil::ReadCache.new(10)
    cache[1] = ReadUntil::ReadChunk.new(channel: 1, id: "r1")
    cache[1] = ReadUntil::ReadChunk.new(channel: 1, id: "r2")
    cache.missed.should eq(1)
    cache.replaced.should eq(0)
  end

  it "evicts the oldest entry when full" do
    cache = ReadUntil::ReadCache.new(2)
    cache[1] = ReadUntil::ReadChunk.new(channel: 1, id: "a")
    cache[2] = ReadUntil::ReadChunk.new(channel: 2, id: "b")
    cache[3] = ReadUntil::ReadChunk.new(channel: 3, id: "c") # evicts channel 1
    cache[1]?.should be_nil
    cache[2]?.should_not be_nil
    cache[3]?.should_not be_nil
    cache.missed.should eq(1)
  end

  it "pops newest entry by default" do
    cache = ReadUntil::ReadCache.new(10)
    cache[1] = ReadUntil::ReadChunk.new(channel: 1, id: "old")
    cache[2] = ReadUntil::ReadChunk.new(channel: 2, id: "new")
    result = cache.pop(1)
    result.size.should eq(1)
    result[0][0].should eq(2)
    result[0][1].id.should eq("new")
    cache.current_size.should eq(1)
  end

  it "pops oldest entry when newest_first is false" do
    cache = ReadUntil::ReadCache.new(10)
    cache[1] = ReadUntil::ReadChunk.new(channel: 1, id: "first")
    cache[2] = ReadUntil::ReadChunk.new(channel: 2, id: "second")
    result = cache.pop(1, newest_first: false)
    result[0][1].id.should eq("first")
  end

  it "pops multiple entries" do
    cache = ReadUntil::ReadCache.new(10)
    (1..5).each { |i| cache[i] = ReadUntil::ReadChunk.new(channel: i, id: "r#{i}") }
    result = cache.pop(3)
    result.size.should eq(3)
    cache.current_size.should eq(2)
  end

  it "returns empty array when cache is empty" do
    cache = ReadUntil::ReadCache.new(10)
    cache.pop(5).should be_empty
  end

  it "clears all entries" do
    cache = ReadUntil::ReadCache.new(10)
    cache[1] = ReadUntil::ReadChunk.new(channel: 1, id: "r1")
    cache.clear
    cache.current_size.should eq(0)
    cache[1]?.should be_nil
  end
end

describe ReadUntil::StreamOptions do
  it "has sensible defaults" do
    opts = ReadUntil::StreamOptions.new
    opts.first_channel.should eq(1)
    opts.last_channel.should eq(512)
    opts.one_chunk.should be_true
    opts.filter_strands.should be_false
  end
end

describe ReadUntil::ReadUntilClient do
  it "exposes endpoint from the connection" do
    config = Minknow::ConnectionConfig.new(host: "localhost", port: 9501, tls: true)
    manager = Minknow::Manager.new(config)
    position = Minknow::FlowCellPosition.new("X4", "localhost", 9600)
    connection = manager.connect(position)

    client = ReadUntil::ReadUntilClient.new(connection: connection)
    client.endpoint.should eq("localhost:9600")
  end

  it "is not running before run is called" do
    config = Minknow::ConnectionConfig.new(host: "localhost", port: 9501, tls: true)
    connection = Minknow::Manager.new(config).connect(
      Minknow::FlowCellPosition.new("X5", "localhost", 9601)
    )
    client = ReadUntil::ReadUntilClient.new(connection: connection)
    client.running?.should be_false
  end

  it "queue_length reflects cache contents" do
    config = Minknow::ConnectionConfig.new(host: "localhost", port: 9501, tls: true)
    connection = Minknow::Manager.new(config).connect(
      Minknow::FlowCellPosition.new("X6", "localhost", 9602)
    )
    client = ReadUntil::ReadUntilClient.new(connection: connection, cache_size: 512)
    client.read_cache[7] = ReadUntil::ReadChunk.new(channel: 7, id: "r1")
    client.queue_length.should eq(1)
  end

  it "read_chunks delegates to cache" do
    config = Minknow::ConnectionConfig.new(host: "localhost", port: 9501, tls: true)
    connection = Minknow::Manager.new(config).connect(
      Minknow::FlowCellPosition.new("X7", "localhost", 9603)
    )
    client = ReadUntil::ReadUntilClient.new(connection: connection)
    client.read_cache[1] = ReadUntil::ReadChunk.new(channel: 1, id: "c1")
    client.read_cache[2] = ReadUntil::ReadChunk.new(channel: 2, id: "c2")
    result = client.read_chunks(1)
    result.size.should eq(1)
  end
end
