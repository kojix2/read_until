module ReadUntil
  class ActionCounters
    def initialize
      @mutex = Mutex.new
      @success = 0_u64
      @failed = 0_u64
    end

    def success : UInt64
      @mutex.synchronize { @success }
    end

    def success=(value : UInt64) : UInt64
      @mutex.synchronize { @success = value }
    end

    def failed : UInt64
      @mutex.synchronize { @failed }
    end

    def failed=(value : UInt64) : UInt64
      @mutex.synchronize { @failed = value }
    end

    def increment_success : Nil
      @mutex.synchronize { @success += 1 }
    end

    def increment_failed : Nil
      @mutex.synchronize { @failed += 1 }
    end
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

    def initialize
      @mutex = Mutex.new
      @bytes_received = 0_u64
      @reads_seen = 0_u64
      @responses_seen = 0_u64
      @samples_since_start = 0_u64
      @seconds_since_start = 0.0_f64
      @samples_behind = 0_u64
      @avg_lag_samples = 0.0_f64
      @lag_measurements = 0_u64
      @actions = ActionStats.new
    end

    def bytes_received : UInt64
      @mutex.synchronize { @bytes_received }
    end

    def reads_seen : UInt64
      @mutex.synchronize { @reads_seen }
    end

    def responses_seen : UInt64
      @mutex.synchronize { @responses_seen }
    end

    def samples_since_start : UInt64
      @mutex.synchronize { @samples_since_start }
    end

    def seconds_since_start : Float64
      @mutex.synchronize { @seconds_since_start }
    end

    def samples_behind : UInt64
      @mutex.synchronize { @samples_behind }
    end

    def avg_lag_samples : Float64
      @mutex.synchronize { @avg_lag_samples }
    end

    def lag_measurements : UInt64
      @mutex.synchronize { @lag_measurements }
    end

    def record_progress(samples_since_start : UInt64, seconds_since_start : Float64) : Nil
      @mutex.synchronize do
        @responses_seen += 1
        @samples_since_start = samples_since_start
        @seconds_since_start = seconds_since_start
      end
    end

    def add_bytes_received(bytes : UInt64) : Nil
      @mutex.synchronize { @bytes_received += bytes }
    end

    def increment_reads_seen : Nil
      @mutex.synchronize { @reads_seen += 1 }
    end

    def observe_lag(samples : UInt64) : Nil
      @mutex.synchronize do
        @samples_behind = samples
        @lag_measurements += 1
        n = @lag_measurements.to_f64
        @avg_lag_samples += (samples.to_f64 - @avg_lag_samples) / n
      end
    end
  end
end
