require "minknow"
require "./read_until/version"

module ReadUntil
  # A single chunk of raw signal data delivered from MinKNOW.
  #
  # Field names follow `GetLiveReadsResponse::ReadData`.
  # `channel` is the map key from the response, injected by the client.
  class ReadChunk
    getter channel : Int32
    getter id : String
    getter chunk_start_sample : UInt64
    getter chunk_length : UInt64
    getter chunk_classifications : Array(Int32)
    getter raw_data : Bytes
    getter median_before : Float32
    getter median : Float32

    def initialize(
      @channel : Int32,
      @id : String,
      @chunk_start_sample : UInt64 = 0_u64,
      @chunk_length : UInt64 = 0_u64,
      @chunk_classifications : Array(Int32) = [] of Int32,
      @raw_data : Bytes = Bytes.empty,
      @median_before : Float32 = 0.0_f32,
      @median : Float32 = 0.0_f32,
    )
    end
  end

  # Thread-safe bounded cache keyed by channel.
  #
  # Keeps the most recent chunk per channel up to *size* total entries.
  # When full, the oldest entry (by insertion order) is evicted.
  # `.missed`   — reads evicted before consumption (distinct read replaced).
  # `.replaced` — chunks overwritten by a later chunk of the same read.
  class ReadCache
    getter size : Int32
    getter missed : Int32 = 0
    getter replaced : Int32 = 0

    def initialize(@size : Int32 = 512)
      raise ArgumentError.new("size must be >= 1") if @size < 1
      @mutex = Mutex.new
      @data = {} of Int32 => ReadChunk
      @order = [] of Int32
    end

    def []?(channel : Int32) : ReadChunk?
      @mutex.synchronize { @data[channel]? }
    end

    def []=(channel : Int32, chunk : ReadChunk) : ReadChunk
      @mutex.synchronize do
        if @data.has_key?(channel)
          existing = @data[channel]
          if existing.id == chunk.id
            @replaced += 1
          else
            @missed += 1
          end
          @order.delete(channel)
        elsif @data.size >= @size
          evicted = @order.shift
          @data.delete(evicted)
          @missed += 1
        end
        @data[channel] = chunk
        @order << channel
      end
      chunk
    end

    def delete(channel : Int32) : ReadChunk?
      @mutex.synchronize do
        chunk = @data.delete(channel)
        @order.delete(channel) if chunk
        chunk
      end
    end

    # Removes and returns up to *count* chunks from the cache.
    # By default returns the newest entries first.
    def pop(count : Int32 = 1, newest_first : Bool = true) : Array({Int32, ReadChunk})
      @mutex.synchronize do
        result = [] of {Int32, ReadChunk}
        count.times do
          key = newest_first ? @order.last? : @order.first?
          break unless key
          chunk = @data.delete(key)
          break unless chunk
          @order.delete(key)
          result << {key, chunk}
        end
        result
      end
    end

    def current_size : Int32
      @mutex.synchronize { @data.size }
    end

    def clear : Nil
      @mutex.synchronize { @data.clear; @order.clear }
    end
  end

  # Configuration for the live-reads stream.
  record StreamOptions,
    first_channel : Int32 = 1,
    last_channel : Int32 = 512,
    sample_minimum_chunk_size : UInt64 = 0_u64,
    accepted_first_chunk_classifications : Array(Int32) = [] of Int32,
    one_chunk : Bool = true,
    filter_strands : Bool = false,
    strand_classifications : Set(Int32) = Set(Int32).new,
    unblock_duration : Float64 = 0.1,
    action_batch_size : Int32 = 1000

  # Live-reads client for MinKNOW adaptive sampling.
  #
  # ```
  # client = ReadUntil::ReadUntilClient.new(connection: conn)
  # client.run do
  #   while client.running?
  #     client.read_chunks(10).each do |channel, chunk|
  #       client.unblock(channel, chunk.id)
  #     end
  #   end
  # end
  # ```
  class ReadUntilClient
    THROTTLE = 0.001 # seconds between action send iterations

    getter connection : Minknow::Connection
    getter options : StreamOptions
    getter read_cache : ReadCache

    @action_queue : Array(MinknowApi::Data::GetLiveReadsRequest::Action)
    @action_mutex : Mutex
    @sent_actions : Hash(String, Symbol)
    @running : Atomic(Int32)
    @action_counter : Atomic(UInt64)

    def initialize(
      @connection : Minknow::Connection,
      @options : StreamOptions = StreamOptions.new,
      cache_size : Int32 = 512,
    )
      @read_cache = ReadCache.new(cache_size)
      @action_queue = [] of MinknowApi::Data::GetLiveReadsRequest::Action
      @action_mutex = Mutex.new
      @sent_actions = {} of String => Symbol
      @running = Atomic(Int32).new(0)
      @action_counter = Atomic(UInt64).new(0_u64)
    end

    def running? : Bool
      @running.get == 1
    end

    def endpoint : String
      connection.endpoint
    end

    def queue_length : Int32
      read_cache.current_size
    end

    def missed_reads : Int32
      read_cache.missed
    end

    def missed_chunks : Int32
      read_cache.replaced
    end

    # Queues an unblock request for *read_id* on *channel*.
    def unblock(channel : Int32, read_id : String, duration : Float64 = options.unblock_duration) : Nil
      enqueue_action(channel, read_id, :unblock, duration: duration)
    end

    # Queues a stop-further-data request for *read_id* on *channel*.
    def stop_receiving(channel : Int32, read_id : String) : Nil
      enqueue_action(channel, read_id, :stop)
    end

    # Opens the bidirectional live-reads stream, starts background fibers
    # for receiving responses and sending batched actions, then yields to
    # the caller's analysis block.  Stops and cleans up when the block returns
    # or raises.
    def run(& : -> Nil) : Nil
      @running.set(1)
      bidi = connection.data.live_reads

      bidi.send(Minknow::DataService.setup_request(
        first_channel: options.first_channel,
        last_channel: options.last_channel,
        sample_minimum_chunk_size: options.sample_minimum_chunk_size,
        accepted_first_chunk_classifications: options.accepted_first_chunk_classifications,
      ))

      spawn { receive_loop(bidi) }
      spawn { send_loop(bidi) }

      begin
        yield
      ensure
        @running.set(0)
        bidi.close_send rescue nil
        bidi.cancel rescue nil
        read_cache.clear
        @action_mutex.synchronize { @action_queue.clear; @sent_actions.clear }
      end
    end

    # Signals the stream to stop without blocking.
    def stop : Nil
      @running.set(0)
    end

    # Removes and returns up to *count* chunks from the cache (newest-first).
    def read_chunks(count : Int32 = 1, newest_first : Bool = true) : Array({Int32, ReadChunk})
      read_cache.pop(count, newest_first)
    end

    private def next_action_id : String
      @action_counter.add(1).to_s
    end

    private def enqueue_action(channel : Int32, read_id : String, kind : Symbol, duration : Float64 = 0.1) : Nil
      id = next_action_id
      proto_action = if kind == :unblock
                       Minknow::DataService.unblock_action(id, channel.to_u32, read_id, duration)
                     else
                       Minknow::DataService.stop_action(id, channel.to_u32, read_id)
                     end
      @action_mutex.synchronize do
        @sent_actions[id] = kind
        @action_queue << proto_action
      end
    end

    private def receive_loop(bidi) : Nil
      unique_ids = Set(String).new
      bidi.each do |response|
        break unless running?
        process_response(response, unique_ids)
      end
    rescue
      stop
    end

    private def send_loop(bidi) : Nil
      while running?
        t0 = Time.monotonic
        batch = @action_mutex.synchronize do
          taken = @action_queue.first(options.action_batch_size)
          @action_queue = @action_queue[taken.size..]? || [] of MinknowApi::Data::GetLiveReadsRequest::Action
          taken
        end
        bidi.send(Minknow::DataService.actions_request(batch)) rescue break unless batch.empty?
        elapsed = (Time.monotonic - t0).total_seconds
        remaining = THROTTLE - elapsed
        sleep(remaining) if remaining > 0
      end
    end

    private def process_response(
      response : MinknowApi::Data::GetLiveReadsResponse,
      unique_ids : Set(String),
    ) : Nil
      response.channels.each do |ch_u32, read_data|
        channel = ch_u32.to_i

        if options.one_chunk
          next if unique_ids.includes?(read_data.id)
          stop_receiving(channel, read_data.id)
          unique_ids.add(read_data.id)
        end

        if options.filter_strands
          next unless read_data.chunk_classifications.any? { |classification| options.strand_classifications.includes?(classification) }
        end

        read_cache[channel] = ReadChunk.new(
          channel: channel,
          id: read_data.id,
          chunk_start_sample: read_data.chunk_start_sample,
          chunk_length: read_data.chunk_length,
          chunk_classifications: read_data.chunk_classifications.dup,
          raw_data: read_data.raw_data.dup,
          median_before: read_data.median_before,
          median: read_data.median,
        )
      end
    end
  end

  Client = ReadUntilClient
end
