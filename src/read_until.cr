require "minknow"
require "./read_until/version"

module ReadUntil
  enum RawDataKind
    None
    Calibrated
    Uncalibrated
  end

  enum StreamMode
    OneChunk
    MultiChunk
  end

  enum PopOrder
    Newest
    Oldest
  end

  enum ActionKind
    Unblock
    Stop
  end

  enum ReadClass
    Strand
    Strand1
    Multiple
    Zero
    Adapter
    MuxUncertain
    User2
    User1
    Event
    Pore
    Unavailable
    Transition
    Unclassed
    Unknown
  end

  record ActionToken, id : String, kind : ActionKind, read : ReadRef

  class AcquisitionProgress
    getter samples_since_start : UInt64
    getter seconds_since_start : Float64

    def initialize(@samples_since_start : UInt64, @seconds_since_start : Float64)
    end
  end

  class ActionCounters
    property success : UInt64 = 0_u64
    property failed : UInt64 = 0_u64
  end

  class ActionStats
    getter unblock : ActionCounters
    getter stop : ActionCounters

    def initialize
      @unblock = ActionCounters.new
      @stop = ActionCounters.new
    end
  end

  class Stats
    getter actions : ActionStats
    property bytes_received : UInt64 = 0_u64
    property reads_seen : UInt64 = 0_u64
    property responses_seen : UInt64 = 0_u64
    property samples_since_start : UInt64 = 0_u64
    property seconds_since_start : Float64 = 0.0_f64

    def initialize
      @actions = ActionStats.new
    end
  end

  class ClassificationRegistry
    DEFAULT_IDS_TO_NAMES = {
      83 => :strand,
      67 => :strand1,
      77 => :multiple,
      90 => :zero,
      65 => :adapter,
      66 => :mux_uncertain,
      70 => :user2,
      68 => :user1,
      69 => :event,
      80 => :pore,
      85 => :unavailable,
      84 => :transition,
      78 => :unclassed,
    }

    SYMBOL_TO_CLASS = {
      :strand        => ReadClass::Strand,
      :strand1       => ReadClass::Strand1,
      :multiple      => ReadClass::Multiple,
      :zero          => ReadClass::Zero,
      :adapter       => ReadClass::Adapter,
      :mux_uncertain => ReadClass::MuxUncertain,
      :user2         => ReadClass::User2,
      :user1         => ReadClass::User1,
      :event         => ReadClass::Event,
      :pore          => ReadClass::Pore,
      :unavailable   => ReadClass::Unavailable,
      :transition    => ReadClass::Transition,
      :unclassed     => ReadClass::Unclassed,
    }

    getter ids_to_names : Hash(Int32, Symbol)
    getter names_to_ids : Hash(Symbol, Int32)

    @@default : ClassificationRegistry?

    def self.default : ClassificationRegistry
      @@default ||= new
    end

    def initialize(ids_to_names : Hash(Int32, Symbol) = DEFAULT_IDS_TO_NAMES.dup)
      @ids_to_names = ids_to_names
      @names_to_ids = {} of Symbol => Int32
      @ids_to_names.each { |id, name| @names_to_ids[name] = id }
    end

    def id_for(name : Symbol) : Int32?
      @names_to_ids[name]?
    end

    def symbol_for(id : Int32) : Symbol?
      @ids_to_names[id]?
    end

    def class_for(id : Int32) : ReadClass
      symbol = symbol_for(id)
      return ReadClass::Unknown unless symbol
      SYMBOL_TO_CLASS[symbol]? || ReadClass::Unknown
    end
  end

  class Prefilter
    getter? enabled : Bool
    getter class_ids : Set(Int32)

    def self.none : Prefilter
      new(false, Set(Int32).new)
    end

    def self.strand_like(*classes : Symbol, registry : ClassificationRegistry = ClassificationRegistry.default) : Prefilter
      class_ids = Set(Int32).new
      classes.each do |name|
        if id = registry.id_for(name)
          class_ids.add(id)
        end
      end
      new(true, class_ids)
    end

    def initialize(@enabled : Bool, @class_ids : Set(Int32))
    end

    def allow?(classification_ids : Array(Int32)) : Bool
      return true unless enabled?
      return false if class_ids.empty?
      classification_ids.any? { |id| class_ids.includes?(id) }
    end
  end

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

  class ReadRef
    getter channel : Int32
    getter id : String?
    getter number : UInt32?

    def initialize(@channel : Int32, @id : String? = nil, @number : UInt32? = nil)
    end
  end

  class Read
    getter channel : Int32
    getter id : String
    getter number : UInt32?
    getter start_sample : UInt64?
    getter chunk_start_sample : UInt64
    getter chunk_length : UInt64
    getter classification_ids : Array(Int32)
    getter raw_bytes : Bytes
    getter median_before : Float32
    getter median : Float32

    @registry : ClassificationRegistry
    @signal_i16 : Array(Int16)?
    @signal_f32 : Array(Float32)?

    def initialize(
      @channel : Int32,
      @id : String,
      @number : UInt32? = nil,
      @start_sample : UInt64? = nil,
      @chunk_start_sample : UInt64 = 0_u64,
      @chunk_length : UInt64 = 0_u64,
      @classification_ids : Array(Int32) = [] of Int32,
      @raw_bytes : Bytes = Bytes.empty,
      @median_before : Float32 = 0.0_f32,
      @median : Float32 = 0.0_f32,
      @registry : ClassificationRegistry = ClassificationRegistry.default,
    )
    end

    def chunk_classifications : Array(Int32)
      classification_ids
    end

    def raw_data : Bytes
      raw_bytes
    end

    def classifications : Array(ReadClass)
      classification_ids.map { |id| @registry.class_for(id) }
    end

    def classification_names : Array(Symbol)
      classification_ids.compact_map { |id| @registry.symbol_for(id) }
    end

    def ref : ReadRef
      ReadRef.new(channel, id, number)
    end

    def strand? : Bool
      classified_as?(:strand)
    end

    def adapter? : Bool
      classified_as?(:adapter)
    end

    def classified_as?(klass : ReadClass) : Bool
      classifications.includes?(klass)
    end

    def classified_as?(klass : Symbol) : Bool
      classification_names.includes?(klass)
    end

    def signal(type : Int16.class) : Slice(Int16)
      decoded = @signal_i16
      unless decoded
        decoded = decode_int16_le(raw_bytes)
        @signal_i16 = decoded
      end
      Slice.new(decoded.to_unsafe, decoded.size)
    end

    def signal(type : Float32.class) : Slice(Float32)
      decoded = @signal_f32
      unless decoded
        decoded = decode_float32_le(raw_bytes)
        @signal_f32 = decoded
      end
      Slice.new(decoded.to_unsafe, decoded.size)
    end

    private def decode_int16_le(bytes : Bytes) : Array(Int16)
      raise ArgumentError.new("raw_data size must be divisible by 2 for Int16") unless bytes.size.divisible_by?(2)
      output = Array(Int16).new(bytes.size // 2)
      i = 0
      while i < bytes.size
        value = IO::ByteFormat::LittleEndian.decode(Int16, bytes[i, 2])
        output << value
        i += 2
      end
      output
    end

    private def decode_float32_le(bytes : Bytes) : Array(Float32)
      raise ArgumentError.new("raw_data size must be divisible by 4 for Float32") unless bytes.size.divisible_by?(4)
      output = Array(Float32).new(bytes.size // 4)
      i = 0
      while i < bytes.size
        value = IO::ByteFormat::LittleEndian.decode(Float32, bytes[i, 4])
        output << value
        i += 4
      end
      output
    end
  end

  abstract class ReadBuffer
    abstract def push(read : Read) : Nil
    abstract def pop(max : Int32 = 1, order : PopOrder = PopOrder::Newest) : Array(Read)
    abstract def clear : Nil
    abstract def size : Int32
    abstract def missed_reads : UInt64
    abstract def replaced_chunks : UInt64
  end

  class ChannelBuffer < ReadBuffer
    getter capacity : Int32
    getter missed_reads : UInt64 = 0_u64
    getter replaced_chunks : UInt64 = 0_u64

    def initialize(@capacity : Int32 = 512)
      raise ArgumentError.new("capacity must be >= 1") if @capacity < 1
      @mutex = Mutex.new
      @data = {} of Int32 => Read
      @order = [] of Int32
    end

    def push(read : Read) : Nil
      @mutex.synchronize do
        channel = read.channel
        if @data.has_key?(channel)
          existing = @data[channel]
          if existing.id == read.id
            @replaced_chunks += 1
          else
            @missed_reads += 1
          end
          @order.delete(channel)
        elsif @data.size >= @capacity
          evicted_channel = @order.shift
          @data.delete(evicted_channel)
          @missed_reads += 1
        end

        @data[channel] = read
        @order << channel
      end
    end

    def pop(max : Int32 = 1, order : PopOrder = PopOrder::Newest) : Array(Read)
      @mutex.synchronize do
        result = [] of Read
        max.times do
          channel = case order
                    when PopOrder::Newest
                      @order.last?
                    when PopOrder::Oldest
                      @order.first?
                    end
          break unless channel
          read = @data.delete(channel)
          break unless read
          @order.delete(channel)
          result << read
        end
        result
      end
    end

    def clear : Nil
      @mutex.synchronize do
        @data.clear
        @order.clear
      end
    end

    def size : Int32
      @mutex.synchronize { @data.size }
    end
  end

  class Session
    getter config : Config
    getter stats : Stats
    getter progress : AcquisitionProgress?
    getter buffer : ReadBuffer

    @connection : Minknow::Connection
    @registry : ClassificationRegistry
    @running : Atomic(Int32)
    @action_queue : Array(MinknowApi::Data::GetLiveReadsRequest::Action)
    @action_mutex : Mutex
    @sent_actions : Hash(String, ActionKind)
    @action_counter : Atomic(UInt64)
    @seen_read_ids : Set(String)
    @bidi : GRPC::BidiCall(MinknowApi::Data::GetLiveReadsRequest, MinknowApi::Data::GetLiveReadsResponse)?

    def initialize(
      @connection : Minknow::Connection,
      @config : Config,
      @buffer : ReadBuffer,
      @registry : ClassificationRegistry = ClassificationRegistry.default,
    )
      @stats = Stats.new
      @progress = nil
      @running = Atomic(Int32).new(0)
      @action_queue = [] of MinknowApi::Data::GetLiveReadsRequest::Action
      @action_mutex = Mutex.new
      @sent_actions = {} of String => ActionKind
      @action_counter = Atomic(UInt64).new(0_u64)
      @seen_read_ids = Set(String).new
      @bidi = nil
    end

    def running? : Bool
      @running.get == 1
    end

    def one_chunk? : Bool
      config.one_chunk?
    end

    def start : self
      @running.set(1)
      bidi = connection.data.live_reads
      @bidi = bidi
      bidi.send(config.to_setup_request)

      spawn { receive_loop(bidi) }
      spawn { send_loop(bidi) }
      self
    end

    def close : Nil
      @running.set(0)
      if bidi = @bidi
        bidi.close_send rescue nil
        bidi.cancel rescue nil
      end
      @buffer.clear
      @action_mutex.synchronize do
        @action_queue.clear
        @sent_actions.clear
      end
      @seen_read_ids.clear
    end

    def stop : Nil
      close
    end

    def each_read(
      batch_size : Int32 = 1,
      order : PopOrder = PopOrder::Newest,
      timeout : Time::Span? = nil,
      & : Read -> Nil
    ) : Nil
      while running?
        reads = pop_reads(max: batch_size, order: order)
        if reads.empty?
          if timeout
            read = next_read?(timeout: timeout)
            break unless read
            yield read
          else
            sleep 1.millisecond
          end
          next
        end
        reads.each { |queued_read| yield queued_read }
      end
    end

    def next_read?(timeout : Time::Span? = nil) : Read?
      started = Time.monotonic
      loop do
        read = pop_reads(max: 1).first?
        return read if read
        return nil unless running?

        if timeout
          elapsed = Time.monotonic - started
          return nil if elapsed >= timeout
        end
        sleep 1.millisecond
      end
    end

    def pop_reads(max : Int32 = 1, order : PopOrder = PopOrder::Newest) : Array(Read)
      @buffer.pop(max, order)
    end

    def unblock(read : Read | ReadRef, for duration : Time::Span = config.unblock_duration) : ActionToken
      read_ref = read.is_a?(Read) ? read.ref : read
      action_id = enqueue_action(read_ref, ActionKind::Unblock, duration)
      ActionToken.new(action_id, ActionKind::Unblock, read_ref)
    end

    def stop(read : Read | ReadRef) : ActionToken
      read_ref = read.is_a?(Read) ? read.ref : read
      action_id = enqueue_action(read_ref, ActionKind::Stop)
      ActionToken.new(action_id, ActionKind::Stop, read_ref)
    end

    def reconfigure(
      *,
      channels : Range(Int32, Int32)? = nil,
      raw_data : RawDataKind? = nil,
      min_chunk_size : UInt64? = nil,
      prefilter : Prefilter? = nil,
    ) : Nil
      @config = @config.with(
        channels: channels,
        raw_data: raw_data,
        min_chunk_size: min_chunk_size,
        prefilter: prefilter,
      )
      @bidi.try &.send(@config.to_setup_request)
    end

    private getter connection : Minknow::Connection

    private def next_action_id : String
      @action_counter.add(1).to_s
    end

    private def enqueue_action(read_ref : ReadRef, kind : ActionKind, duration : Time::Span? = nil) : String
      read_id = read_ref.id
      raise ArgumentError.new("read reference must include read id") unless read_id

      action_id = next_action_id
      proto_action = case kind
                     when ActionKind::Unblock
                       action_duration = (duration || config.unblock_duration).total_seconds
                       Minknow::DataService.unblock_action(action_id, read_ref.channel.to_u32, read_id, action_duration)
                     when ActionKind::Stop
                       Minknow::DataService.stop_action(action_id, read_ref.channel.to_u32, read_id)
                     end

      @action_mutex.synchronize do
        @sent_actions[action_id] = kind
        @action_queue << proto_action
      end
      action_id
    end

    private def receive_loop(bidi : GRPC::BidiCall(MinknowApi::Data::GetLiveReadsRequest, MinknowApi::Data::GetLiveReadsResponse)) : Nil
      bidi.each do |response|
        break unless running?
        process_response(response)
      end
    rescue
      @running.set(0)
    end

    private def send_loop(bidi : GRPC::BidiCall(MinknowApi::Data::GetLiveReadsRequest, MinknowApi::Data::GetLiveReadsResponse)) : Nil
      while running?
        started = Time.monotonic
        batch = @action_mutex.synchronize do
          taken = @action_queue.first(config.action_batch_size)
          @action_queue = @action_queue[taken.size..]? || [] of MinknowApi::Data::GetLiveReadsRequest::Action
          taken
        end

        unless batch.empty?
          bidi.send(Minknow::DataService.actions_request(batch))
        end

        elapsed = Time.monotonic - started
        sleep_for = config.action_throttle - elapsed
        sleep(sleep_for) if sleep_for > 0.seconds
      end
    rescue
      @running.set(0)
    end

    private def process_response(response : MinknowApi::Data::GetLiveReadsResponse) : Nil
      stats.responses_seen += 1
      stats.samples_since_start = response.samples_since_start
      stats.seconds_since_start = response.seconds_since_start
      @progress = AcquisitionProgress.new(response.samples_since_start, response.seconds_since_start)

      response.action_responses.each do |reply|
        kind = @action_mutex.synchronize { @sent_actions.delete(reply.action_id) }
        next unless kind

        success = reply.response.raw == MinknowApi::Data::GetLiveReadsResponse::ActionResponse::Response::SUCCESS.value
        counters = kind == ActionKind::Unblock ? stats.actions.unblock : stats.actions.stop
        if success
          counters.success += 1
        else
          counters.failed += 1
        end
      end

      response.channels.each do |channel_u32, data|
        next if config.one_chunk? && @seen_read_ids.includes?(data.id)
        @seen_read_ids.add(data.id) if config.one_chunk?

        if config.one_chunk?
          stop(ReadRef.new(channel_u32.to_i, data.id))
        end

        classification_ids = data.chunk_classifications.dup
        next unless config.prefilter.allow?(classification_ids)

        read = Read.new(
          channel: channel_u32.to_i,
          id: data.id,
          start_sample: data.start_sample,
          chunk_start_sample: data.chunk_start_sample,
          chunk_length: data.chunk_length,
          classification_ids: classification_ids,
          raw_bytes: data.raw_data.dup,
          median_before: data.median_before,
          median: data.median,
          registry: @registry,
        )
        @buffer.push(read)
        stats.bytes_received += read.raw_bytes.size.to_u64
        stats.reads_seen += 1
      end
    end
  end

  class SignalFormat
    getter calibrated : String
    getter uncalibrated : String

    def initialize(@calibrated : String = "float32", @uncalibrated : String = "int16")
    end
  end

  class Client
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
      @signal_format = SignalFormat.new
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
      session = new_session(
        channels: channels,
        raw_data: raw_data,
        min_chunk_size: min_chunk_size,
        mode: mode,
        prefilter: prefilter,
        buffer: buffer,
        action_batch_size: action_batch_size,
        action_throttle: action_throttle,
        unblock_duration: unblock_duration,
      )

      begin
        session.start
        yield session
      ensure
        session.close
      end
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
      Session.new(connection, config, buffer, classifications)
    end
  end
end
