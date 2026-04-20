module ReadUntil
  class Config
    getter channels : Range(Int32, Int32)
    getter raw_data : RawDataKind
    getter min_chunk_size : UInt64
    getter mode : StreamMode
    getter prefilter : Prefilter
    getter action_batch_size : Int32
    getter action_throttle : Time::Span
    getter unblock_duration : Time::Span

    def initialize(
      @channels : Range(Int32, Int32) = 1..512,
      @raw_data : RawDataKind = RawDataKind::Calibrated,
      @min_chunk_size : UInt64 = 0_u64,
      @mode : StreamMode = StreamMode::OneChunk,
      @prefilter : Prefilter = Prefilter.none,
      @action_batch_size : Int32 = 1000,
      @action_throttle : Time::Span = 1.millisecond,
      @unblock_duration : Time::Span = 100.milliseconds,
    )
      raise ArgumentError.new("channels must not be empty") if channels.begin > channels.end
      raise ArgumentError.new("action_batch_size must be >= 1") if action_batch_size < 1
    end

    def one_chunk? : Bool
      mode == StreamMode::OneChunk
    end

    def first_channel : Int32
      channels.begin
    end

    def last_channel : Int32
      channels.end
    end

    def with(
      *,
      channels : Range(Int32, Int32)? = nil,
      raw_data : RawDataKind? = nil,
      min_chunk_size : UInt64? = nil,
      prefilter : Prefilter? = nil,
    ) : Config
      Config.new(
        channels: channels || @channels,
        raw_data: raw_data || @raw_data,
        min_chunk_size: min_chunk_size || @min_chunk_size,
        mode: @mode,
        prefilter: prefilter || @prefilter,
        action_batch_size: @action_batch_size,
        action_throttle: @action_throttle,
        unblock_duration: @unblock_duration,
      )
    end

    def to_setup_request : MinknowApi::Data::GetLiveReadsRequest
      raw_data_type = case raw_data
                      when RawDataKind::None
                        Minknow::DataService::RawDataType::NONE
                      when RawDataKind::Uncalibrated
                        Minknow::DataService::RawDataType::UNCALIBRATED
                      else
                        Minknow::DataService::RawDataType::CALIBRATED
                      end

      Minknow::DataService.setup_request(
        first_channel: first_channel,
        last_channel: last_channel,
        raw_data_type: raw_data_type,
        sample_minimum_chunk_size: min_chunk_size,
        accepted_first_chunk_classifications: prefilter.class_ids.to_a,
      )
    end
  end
end
