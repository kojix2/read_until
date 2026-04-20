require "./spec_helper"

describe ReadUntil do
  it "has a version number" do
    ReadUntil::VERSION.should be_a(String)
  end
end

describe ReadUntil::Read do
  it "stores core fields and exposes read helpers" do
    read = ReadUntil::Read.new(
      channel: 3,
      id: "read-abc",
      start_sample: 1000_u64,
      chunk_start_sample: 1000_u64,
      chunk_length: 200_u64,
      classification_ids: [83, 65],
      raw_bytes: Bytes[1, 2, 3],
      median_before: 180.5_f32,
      median: 75.0_f32,
    )

    read.channel.should eq(3)
    read.id.should eq("read-abc")
    read.start_sample.should eq(1000_u64)
    read.chunk_length.should eq(200_u64)
    read.strand?.should be_true
    read.adapter?.should be_true
    read.classified_as?(ReadUntil::ReadClass::Strand).should be_true
    read.classified_as?(:adapter).should be_true
    read.ref.channel.should eq(3)
    read.ref.id.should eq("read-abc")
  end

  it "decodes Int16 little-endian signal" do
    raw = Bytes[0x01, 0x00, 0xFF, 0x7F]
    fmt = ReadUntil::SignalFormat.new(calibrated: "float32", uncalibrated: "int16")
    read = ReadUntil::Read.new(channel: 1, id: "r1", raw_bytes: raw,
      signal_format: fmt, raw_data_kind: ReadUntil::RawDataKind::Uncalibrated)
    signal = read.signal(Int16)

    signal.size.should eq(2)
    signal[0].should eq(1_i16)
    signal[1].should eq(32_767_i16)
  end

  it "decodes Float32 little-endian signal" do
    io = IO::Memory.new
    IO::ByteFormat::LittleEndian.encode(1.5_f32, io)
    IO::ByteFormat::LittleEndian.encode(-2.25_f32, io)
    fmt = ReadUntil::SignalFormat.new(calibrated: "float32", uncalibrated: "int16")
    read = ReadUntil::Read.new(channel: 1, id: "r2", raw_bytes: io.to_slice,
      signal_format: fmt, raw_data_kind: ReadUntil::RawDataKind::Calibrated)

    signal = read.signal(Float32)
    signal.size.should eq(2)
    signal[0].should be_close(1.5_f32, 0.0001)
    signal[1].should be_close(-2.25_f32, 0.0001)
  end

  it "exposes calibrated and uncalibrated signal sugar" do
    fmt = ReadUntil::SignalFormat.new(calibrated: "float32", uncalibrated: "int16")
    raw_i16 = Bytes[0x01, 0x00, 0x02, 0x00]
    read_i16 = ReadUntil::Read.new(channel: 1, id: "ri16", raw_bytes: raw_i16,
      signal_format: fmt, raw_data_kind: ReadUntil::RawDataKind::Uncalibrated)
    read_i16.uncalibrated_signal.to_a.should eq([1_i16, 2_i16])

    io = IO::Memory.new
    IO::ByteFormat::LittleEndian.encode(1.5_f32, io)
    read_f32 = ReadUntil::Read.new(channel: 1, id: "rf32", raw_bytes: io.to_slice,
      signal_format: fmt, raw_data_kind: ReadUntil::RawDataKind::Calibrated)
    read_f32.calibrated_signal[0].should be_close(1.5_f32, 0.0001)
  end

  it "raises when signal type mismatches format" do
    raw = Bytes[0x01, 0x00, 0x02, 0x00]
    fmt = ReadUntil::SignalFormat.new(calibrated: "float32", uncalibrated: "int16")

    # Calibrated bytes (float32) but requesting Int16
    read_cal = ReadUntil::Read.new(channel: 1, id: "rc", raw_bytes: raw,
      signal_format: fmt, raw_data_kind: ReadUntil::RawDataKind::Calibrated)
    expect_raises(ArgumentError, /float32/) { read_cal.signal(Int16) }
    expect_raises(ArgumentError, /Calibrated/) { read_cal.uncalibrated_signal }

    # Uncalibrated bytes (int16) but requesting Float32
    read_unc = ReadUntil::Read.new(channel: 1, id: "ru", raw_bytes: raw,
      signal_format: fmt, raw_data_kind: ReadUntil::RawDataKind::Uncalibrated)
    expect_raises(ArgumentError, /int16/) { read_unc.signal(Float32) }
    expect_raises(ArgumentError, /Uncalibrated/) { read_unc.calibrated_signal }
  end
end

describe ReadUntil::Stats do
  it "tracks lag samples and moving average" do
    stats = ReadUntil::Stats.new
    stats.observe_lag(10_u64)
    stats.observe_lag(20_u64)

    stats.samples_behind.should eq(20_u64)
    stats.lag_measurements.should eq(2_u64)
    stats.avg_lag_samples.should be_close(15.0_f64, 0.0001)
  end
end

describe ReadUntil::Prefilter do
  it "accepts strand-like reads when configured" do
    prefilter = ReadUntil::Prefilter.strand_like(:strand, :adapter)

    prefilter.allow?([83]).should be_true
    prefilter.allow?([65]).should be_true
    prefilter.allow?([78]).should be_false
  end

  it "accepts ReadClass and mixed class inputs" do
    typed = ReadUntil::Prefilter.strand_like(ReadUntil::ReadClass::Strand, ReadUntil::ReadClass::Adapter)
    mixed = ReadUntil::Prefilter.strand_like(ReadUntil::ReadClass::Strand, :adapter)

    typed.allow?([83]).should be_true
    typed.allow?([65]).should be_true
    mixed.allow?([65]).should be_true
  end

  it "allows all when disabled" do
    ReadUntil::Prefilter.none.allow?([999]).should be_true
  end
end

describe ReadUntil::ChannelBuffer do
  it "raises when capacity < 1" do
    expect_raises(ArgumentError) { ReadUntil::ChannelBuffer.new(0) }
  end

  it "stores latest read per channel" do
    buffer = ReadUntil::ChannelBuffer.new(10)
    buffer.push(ReadUntil::Read.new(channel: 1, id: "r1"))
    buffer.push(ReadUntil::Read.new(channel: 1, id: "r2"))

    popped = buffer.pop(1)
    popped.size.should eq(1)
    popped[0].id.should eq("r2")
  end

  it "counts replaced chunks from same read" do
    buffer = ReadUntil::ChannelBuffer.new(10)
    buffer.push(ReadUntil::Read.new(channel: 1, id: "r1"))
    buffer.push(ReadUntil::Read.new(channel: 1, id: "r1"))

    buffer.replaced_chunks.should eq(1)
    buffer.missed_reads.should eq(0)
  end

  it "counts missed reads for distinct overwrites" do
    buffer = ReadUntil::ChannelBuffer.new(10)
    buffer.push(ReadUntil::Read.new(channel: 1, id: "r1"))
    buffer.push(ReadUntil::Read.new(channel: 1, id: "r2"))

    buffer.missed_reads.should eq(1)
    buffer.replaced_chunks.should eq(0)
  end

  it "evicts oldest when full" do
    buffer = ReadUntil::ChannelBuffer.new(2)
    buffer.push(ReadUntil::Read.new(channel: 1, id: "a"))
    buffer.push(ReadUntil::Read.new(channel: 2, id: "b"))
    buffer.push(ReadUntil::Read.new(channel: 3, id: "c"))

    popped = buffer.pop(2, order: ReadUntil::PopOrder::Oldest)
    popped.map(&.channel).should eq([2, 3])
    buffer.missed_reads.should eq(1)
  end

  it "waits for incoming reads" do
    buffer = ReadUntil::ChannelBuffer.new(2)

    spawn do
      sleep 5.milliseconds
      buffer.push(ReadUntil::Read.new(channel: 1, id: "delayed"))
    end

    buffer.wait_for_read(100.milliseconds).should be_true
    buffer.pop(1).first.try(&.id).should eq("delayed")
  end
end

describe ReadUntil::Config do
  it "has sensible defaults" do
    cfg = ReadUntil::Config.new

    cfg.channels.should eq(1..512)
    cfg.raw_data.should eq(ReadUntil::RawDataKind::Calibrated)
    cfg.one_chunk?.should be_true
  end

  it "can be reconfigured with #with" do
    cfg = ReadUntil::Config.new
    updated = cfg.with(channels: 5..10, min_chunk_size: 20_u64)

    updated.channels.should eq(5..10)
    updated.min_chunk_size.should eq(20_u64)
    updated.mode.should eq(cfg.mode)
  end
end

describe ReadUntil::SignalFormat do
  it "builds format strings from MinKNOW data types" do
    calibrated = MinknowApi::Data::GetDataTypesResponse::DataType.new
    calibrated.type_ = Proto::OpenEnum(MinknowApi::Data::GetDataTypesResponse::DataType::Type).new(
      MinknowApi::Data::GetDataTypesResponse::DataType::Type::FLOATING_POINT.value
    )
    calibrated.size = 4

    uncalibrated = MinknowApi::Data::GetDataTypesResponse::DataType.new
    uncalibrated.type_ = Proto::OpenEnum(MinknowApi::Data::GetDataTypesResponse::DataType::Type).new(
      MinknowApi::Data::GetDataTypesResponse::DataType::Type::SIGNED_INTEGER.value
    )
    uncalibrated.size = 2

    types = MinknowApi::Data::GetDataTypesResponse.new
    types.calibrated_signal = calibrated
    types.uncalibrated_signal = uncalibrated

    format = ReadUntil::SignalFormat.from_data_types(types)
    format.calibrated.should eq("float32")
    format.uncalibrated.should eq("int16")
  end

  it "marks big-endian types" do
    data_type = MinknowApi::Data::GetDataTypesResponse::DataType.new
    data_type.type_ = Proto::OpenEnum(MinknowApi::Data::GetDataTypesResponse::DataType::Type).new(
      MinknowApi::Data::GetDataTypesResponse::DataType::Type::UNSIGNED_INTEGER.value
    )
    data_type.size = 2
    data_type.big_endian = true

    ReadUntil::SignalFormat.dtype_name(data_type).should eq("uint16_be")
  end
end

describe ReadUntil::Client do
  it "creates a session with expected defaults" do
    config = Minknow::ConnectionConfig.new(host: "localhost", port: 9501, tls: true)
    manager = Minknow::Manager.new(config)
    position = Minknow::FlowCellPosition.new("X4", "localhost", 9600)
    connection = manager.connect(position)

    client = ReadUntil::Client.new(connection)
    session = client.new_session

    session.running?.should be_false
    session.one_chunk?.should be_true
    session.config.channels.should eq(1..512)
  end

  it "creates a session from Config" do
    config = Minknow::ConnectionConfig.new(host: "localhost", port: 9501, tls: true)
    manager = Minknow::Manager.new(config)
    position = Minknow::FlowCellPosition.new("X5", "localhost", 9601)
    connection = manager.connect(position)

    client = ReadUntil::Client.new(connection)
    stream_config = ReadUntil::Config.new(
      channels: 10..20,
      mode: ReadUntil::StreamMode::MultiChunk,
      prefilter: ReadUntil::Prefilter.strand_like(ReadUntil::ReadClass::Adapter),
    )

    session = client.new_session(stream_config)
    session.config.channels.should eq(10..20)
    session.one_chunk?.should be_false
    session.config.prefilter.allow?([65]).should be_true
  end
end
