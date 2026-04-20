module ReadUntil
  class Client
    private Log = ::Log.for(self)

    getter connection : Minknow::Connection
    getter classifications : ClassificationRegistry
    getter signal_format : SignalFormat

    def endpoint : String
      connection.endpoint
    end

    def initialize(
      @connection : Minknow::Connection,
      @classifications : ClassificationRegistry = ClassificationRegistry.default,
    )
      @signal_format = fetch_signal_format
    end

    private def fetch_signal_format : SignalFormat
      SignalFormat.from_data_types(connection.data.data_types)
    rescue ex
      Log.warn(exception: ex) { "failed to fetch signal format from device; falling back to defaults" }
      SignalFormat.new
    end

    def open(
      config : Config,
      buffer : ReadBuffer = ChannelBuffer.new(capacity: 512),
      & : Session -> Nil
    ) : Nil
      session = new_session(config, buffer: buffer)

      begin
        session.start
        yield session
      ensure
        session.close
      end
    end

    def open(
      *,
      channels : Range(Int32, Int32) = 1..512,
      raw_data : RawDataKind = RawDataKind::Calibrated,
      min_chunk_size : UInt64 = 0_u64,
      mode : StreamMode = StreamMode::OneChunk,
      prefilter : Prefilter = Prefilter.none,
      buffer : ReadBuffer = ChannelBuffer.new(capacity: 512),
      action_batch_size : Int32 = 1000,
      action_throttle : Time::Span = 1.millisecond,
      unblock_duration : Time::Span = 100.milliseconds,
      & : Session -> Nil
    ) : Nil
      config = Config.new(
        channels: channels,
        raw_data: raw_data,
        min_chunk_size: min_chunk_size,
        mode: mode,
        prefilter: prefilter,
        action_batch_size: action_batch_size,
        action_throttle: action_throttle,
        unblock_duration: unblock_duration,
      )
      open(config, buffer: buffer) { |session| yield session }
    end

    def new_session(
      config : Config,
      buffer : ReadBuffer = ChannelBuffer.new(capacity: 512),
    ) : Session
      Session.new(connection, config, buffer, classifications, signal_format)
    end

    def new_session(
      *,
      channels : Range(Int32, Int32) = 1..512,
      raw_data : RawDataKind = RawDataKind::Calibrated,
      min_chunk_size : UInt64 = 0_u64,
      mode : StreamMode = StreamMode::OneChunk,
      prefilter : Prefilter = Prefilter.none,
      buffer : ReadBuffer = ChannelBuffer.new(capacity: 512),
      action_batch_size : Int32 = 1000,
      action_throttle : Time::Span = 1.millisecond,
      unblock_duration : Time::Span = 100.milliseconds,
    ) : Session
      config = Config.new(
        channels: channels,
        raw_data: raw_data,
        min_chunk_size: min_chunk_size,
        mode: mode,
        prefilter: prefilter,
        action_batch_size: action_batch_size,
        action_throttle: action_throttle,
        unblock_duration: unblock_duration,
      )
      new_session(config, buffer: buffer)
    end
  end
end
