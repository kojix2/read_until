module ReadUntil
  class Session
    private Log                 = ::Log.for(self)
    private LoopShutdownTimeout = 1.second

    getter stats : Stats
    getter buffer : ReadBuffer

    @connection : Minknow::Connection
    @registry : ClassificationRegistry
    @signal_format : SignalFormat
    @state_mutex : Mutex
    @running : Atomic(Int32)
    @action_queue : Array(MinknowApi::Data::GetLiveReadsRequest::Action)
    @action_mutex : Mutex
    @sent_actions : Hash(String, ActionKind)
    @action_counter : Atomic(UInt64)
    @last_read_ids_by_channel : Hash(Int32, String)
    @last_read_ids_mutex : Mutex
    @bidi : GRPC::BidiCall(MinknowApi::Data::GetLiveReadsRequest, MinknowApi::Data::GetLiveReadsResponse)?
    @receive_done : Channel(Nil)?
    @send_done : Channel(Nil)?
    @stop_signal : Channel(Nil)?

    def initialize(
      @connection : Minknow::Connection,
      @config : Config,
      @buffer : ReadBuffer,
      @registry : ClassificationRegistry = ClassificationRegistry.default,
      @signal_format : SignalFormat = SignalFormat.new,
    )
      @state_mutex = Mutex.new
      @stats = Stats.new
      @progress = nil
      @running = Atomic(Int32).new(0)
      @action_queue = [] of MinknowApi::Data::GetLiveReadsRequest::Action
      @action_mutex = Mutex.new
      @sent_actions = {} of String => ActionKind
      @action_counter = Atomic(UInt64).new(0_u64)
      @last_read_ids_by_channel = {} of Int32 => String
      @last_read_ids_mutex = Mutex.new
      @bidi = nil
      @receive_done = nil
      @send_done = nil
      @stop_signal = nil
    end

    def running? : Bool
      @running.get == 1
    end

    def one_chunk? : Bool
      config.one_chunk?
    end

    def config : Config
      @state_mutex.synchronize { @config }
    end

    def progress : AcquisitionProgress?
      @state_mutex.synchronize { @progress }
    end

    def start : self
      unless @running.compare_and_set(0, 1)
        raise ArgumentError.new("session is already running")
      end

      receive_done = Channel(Nil).new(1)
      send_done = Channel(Nil).new(1)
      stop_signal = Channel(Nil).new(1)

      bidi = connection.data.live_reads
      begin
        @state_mutex.synchronize do
          @receive_done = receive_done
          @send_done = send_done
          @stop_signal = stop_signal
          @bidi = bidi
        end
        bidi.send(config.to_setup_request)

        spawn { receive_loop(bidi, receive_done) }
        spawn { send_loop(bidi, send_done, stop_signal) }
      rescue ex
        @state_mutex.synchronize do
          @receive_done = nil
          @send_done = nil
          @stop_signal = nil
          @bidi = nil
        end
        @running.set(0)
        begin
          bidi.close_send
        rescue
        end
        begin
          bidi.cancel
        rescue
        end
        raise ex
      end

      self
    end

    def close : Nil
      @running.set(0)
      stop_signal = nil
      receive_done = nil
      send_done = nil
      bidi = nil
      @state_mutex.synchronize do
        stop_signal = @stop_signal
        receive_done = @receive_done
        send_done = @send_done
        bidi = @bidi
        @receive_done = nil
        @send_done = nil
        @bidi = nil
      end

      stop_signal.try do |signal|
        signal.close
      rescue Channel::ClosedError
      end

      if bidi
        begin
          bidi.close_send
        rescue ex
          Log.debug(exception: ex) { "failed to close send side of live reads stream" }
        end
        begin
          bidi.cancel
        rescue ex
          Log.debug(exception: ex) { "failed to cancel live reads stream" }
        end
      end

      wait_for_loop(receive_done, "receive")
      wait_for_loop(send_done, "send")
      @state_mutex.synchronize do
        @stop_signal = nil
      end

      @buffer.clear
      @action_mutex.synchronize do
        @action_queue.clear
        @sent_actions.clear
      end
      @last_read_ids_mutex.synchronize do
        @last_read_ids_by_channel.clear
      end
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
      loop do
        reads = pop_reads(max: batch_size, order: order)
        if reads.empty?
          read = next_read?(timeout: timeout)
          break unless read
          yield read
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

        remaining = timeout
        if remaining
          elapsed = Time.monotonic - started
          remaining -= elapsed
          return nil if remaining <= 0.seconds
        end

        return nil unless wait_for_next_read(remaining)
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
      config, bidi = @state_mutex.synchronize do
        @config = @config.with(
          channels: channels,
          raw_data: raw_data,
          min_chunk_size: min_chunk_size,
          prefilter: prefilter,
        )
        {@config, @bidi}
      end
      bidi.try &.send(config.to_setup_request)
    end

    private getter connection : Minknow::Connection

    private def next_action_id : String
      @action_counter.add(1).to_s
    end

    private def enqueue_action(read_ref : ReadRef, kind : ActionKind, duration : Time::Span? = nil) : String
      read_id = read_ref.id
      unless read_id
        raise ArgumentError.new("read reference must include read id")
      end

      action_id = next_action_id
      proto_action = case kind
                     when ActionKind::Unblock
                       action_duration = (duration || config.unblock_duration).total_seconds
                       Minknow::DataService.unblock_action(action_id, read_ref.channel.to_u32, read_id, action_duration)
                     when ActionKind::Stop
                       Minknow::DataService.stop_action(action_id, read_ref.channel.to_u32, read_id)
                     else
                       raise ArgumentError.new("unknown action kind: #{kind}")
                     end

      @action_mutex.synchronize do
        @sent_actions[action_id] = kind
        @action_queue << proto_action
      end
      action_id
    end

    private def receive_loop(
      bidi : GRPC::BidiCall(MinknowApi::Data::GetLiveReadsRequest, MinknowApi::Data::GetLiveReadsResponse),
      done : Channel(Nil),
    ) : Nil
      bidi.each do |response|
        break unless running?
        process_response(response)
      end
    rescue ex
      if running?
        Log.error(exception: ex) { "receive loop crashed" }
        @running.set(0)
      else
        Log.debug(exception: ex) { "receive loop stopped after shutdown" }
      end
    ensure
      done.send(nil) rescue nil
    end

    private def send_loop(
      bidi : GRPC::BidiCall(MinknowApi::Data::GetLiveReadsRequest, MinknowApi::Data::GetLiveReadsResponse),
      done : Channel(Nil),
      stop_signal : Channel(Nil),
    ) : Nil
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
        if sleep_for > 0.seconds
          select
          when stop_signal.receive?
            break
          when timeout(sleep_for)
          end
        end
      end
    rescue ex
      if running?
        Log.error(exception: ex) { "send loop crashed" }
        @running.set(0)
      else
        Log.debug(exception: ex) { "send loop stopped after shutdown" }
      end
    ensure
      done.send(nil) rescue nil
    end

    private def process_response(response : MinknowApi::Data::GetLiveReadsResponse) : Nil
      return unless running?

      current_config = config

      stats.record_progress(response.samples_since_start, response.seconds_since_start)
      @state_mutex.synchronize do
        @progress = AcquisitionProgress.new(response.samples_since_start, response.seconds_since_start)
      end

      response.action_responses.each do |reply|
        kind = @action_mutex.synchronize { @sent_actions.delete(reply.action_id) }
        next unless kind

        success = reply.response.raw == MinknowApi::Data::GetLiveReadsResponse::ActionResponse::Response::SUCCESS.value
        counters = kind == ActionKind::Unblock ? stats.actions.unblock : stats.actions.stop
        if success
          counters.increment_success
        else
          counters.increment_failed
        end
      end

      response.channels.each do |channel_u32, data|
        channel = channel_u32.to_i
        should_skip = @last_read_ids_mutex.synchronize do
          if current_config.one_chunk?
            previous_id = @last_read_ids_by_channel[channel]?
            if previous_id == data.id
              true
            else
              @last_read_ids_by_channel[channel] = data.id
              false
            end
          else
            false
          end
        end
        next if should_skip

        if current_config.one_chunk?
          stop(ReadRef.new(channel, data.id))
        end

        classification_ids = data.chunk_classifications.dup
        next unless current_config.prefilter.allow?(classification_ids)

        read = Read.new(
          channel: channel,
          id: data.id,
          start_sample: data.start_sample,
          chunk_start_sample: data.chunk_start_sample,
          chunk_length: data.chunk_length,
          classification_ids: classification_ids,
          raw_bytes: data.raw_data.dup,
          median_before: data.median_before,
          median: data.median,
          registry: @registry,
          signal_format: @signal_format,
          raw_data_kind: current_config.raw_data,
        )

        read_end = data.chunk_start_sample + data.chunk_length
        lag = response.samples_since_start > read_end ? response.samples_since_start - read_end : 0_u64
        stats.observe_lag(lag)

        @buffer.push(read)
        stats.add_bytes_received(read.raw_bytes.size.to_u64)
        stats.increment_reads_seen
      end
    end

    private def wait_for_loop(done : Channel(Nil)?, loop_name : String) : Nil
      return unless done

      select
      when done.receive?
      when timeout(LoopShutdownTimeout)
        Log.warn { "#{loop_name} loop did not stop within #{LoopShutdownTimeout}" }
      end
    end

    private def wait_for_next_read(timeout : Time::Span?) : Bool
      stop_signal = @state_mutex.synchronize { @stop_signal }

      case current_buffer = @buffer
      when ChannelBuffer
        current_buffer.wait_for_read(timeout, stop_signal)
      else
        wait_for_next_read_with_polling(timeout)
      end
    end

    private def wait_for_next_read_with_polling(timeout : Time::Span?) : Bool
      started = Time.monotonic
      loop do
        return true if @buffer.size > 0
        return false unless running?

        sleep_for = 1.millisecond
        if timeout
          elapsed = Time.monotonic - started
          remaining = timeout - elapsed
          return false if remaining <= 0.seconds
          sleep_for = remaining if remaining < sleep_for
        end

        sleep(sleep_for)
      end
    end
  end
end
