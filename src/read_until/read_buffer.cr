module ReadUntil
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

    def initialize(@capacity : Int32 = 512)
      raise ArgumentError.new("capacity must be >= 1") if @capacity < 1
      @mutex = Mutex.new
      @data = {} of Int32 => Read
      @order = [] of Int32
      @missed_reads = 0_u64
      @replaced_chunks = 0_u64
      @ready_signal = Channel(Nil).new(1)
    end

    def missed_reads : UInt64
      @mutex.synchronize { @missed_reads }
    end

    def replaced_chunks : UInt64
      @mutex.synchronize { @replaced_chunks }
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

      notify_read_available
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

    def wait_for_read(timeout : Time::Span? = nil, stop_signal : Channel(Nil)? = nil) : Bool
      return true if size > 0

      if timeout
        if stop_signal
          select
          when @ready_signal.receive?
            true
          when stop_signal.receive?
            false
          when timeout(timeout)
            false
          end
        else
          select
          when @ready_signal.receive?
            true
          when timeout(timeout)
            false
          end
        end
      elsif stop_signal
        select
        when @ready_signal.receive?
          true
        when stop_signal.receive?
          false
        end
      else
        @ready_signal.receive?
        true
      end
    end

    private def notify_read_available : Nil
      select
      when @ready_signal.send(nil)
      when timeout(0.seconds)
      end
    end
  end
end
