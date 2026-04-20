module ReadUntil
  # Read reference identified by channel + read id.
  # Note: Future support for read number-based references may be added.
  # Currently hidden to avoid incomplete API surface.
  class ReadRef
    getter channel : Int32
    getter id : String?

    def initialize(@channel : Int32, @id : String? = nil)
    end
  end

  # Live read chunk with signal data and metadata.
  # Signal access is guarded by format validation to ensure consistency.
  # Note: Read number tracking is reserved for future implementation;
  # actions currently require explicit read id.
  class Read
    getter channel : Int32
    getter id : String
    getter start_sample : UInt64?
    getter chunk_start_sample : UInt64
    getter chunk_length : UInt64
    getter classification_ids : Array(Int32)
    getter raw_bytes : Bytes
    getter median_before : Float32
    getter median : Float32
    getter signal_format : SignalFormat
    getter raw_data_kind : RawDataKind

    @registry : ClassificationRegistry
    @signal_mutex : Mutex
    @signal_i16 : Array(Int16)?
    @signal_f32 : Array(Float32)?

    def initialize(
      @channel : Int32,
      @id : String,
      @start_sample : UInt64? = nil,
      @chunk_start_sample : UInt64 = 0_u64,
      @chunk_length : UInt64 = 0_u64,
      @classification_ids : Array(Int32) = [] of Int32,
      @raw_bytes : Bytes = Bytes.empty,
      @median_before : Float32 = 0.0_f32,
      @median : Float32 = 0.0_f32,
      @registry : ClassificationRegistry = ClassificationRegistry.default,
      @signal_format : SignalFormat = SignalFormat.new,
      @raw_data_kind : RawDataKind = RawDataKind::Calibrated,
    )
      @signal_mutex = Mutex.new
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
      ReadRef.new(channel, id)
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
      dtype = signal_format.dtype_for(raw_data_kind)
      raise ArgumentError.new("signal(Int16) requested but device format is '#{dtype}'") unless dtype == "int16"
      decoded = @signal_mutex.synchronize do
        @signal_i16 ||= decode_int16_le(raw_bytes)
      end
      Slice.new(decoded.to_unsafe, decoded.size)
    end

    def signal(type : Float32.class) : Slice(Float32)
      dtype = signal_format.dtype_for(raw_data_kind)
      raise ArgumentError.new("signal(Float32) requested but device format is '#{dtype}'") unless dtype == "float32"
      decoded = @signal_mutex.synchronize do
        @signal_f32 ||= decode_float32_le(raw_bytes)
      end
      Slice.new(decoded.to_unsafe, decoded.size)
    end

    def calibrated_signal : Slice(Float32)
      raise ArgumentError.new("raw_data_kind is #{raw_data_kind}; use uncalibrated_signal or signal(T)") unless raw_data_kind == RawDataKind::Calibrated
      raise ArgumentError.new("calibrated signal format is '#{signal_format.calibrated}', not float32") unless signal_format.calibrated_float32?
      signal(Float32)
    end

    def uncalibrated_signal : Slice(Int16)
      raise ArgumentError.new("raw_data_kind is #{raw_data_kind}; use calibrated_signal or signal(T)") unless raw_data_kind == RawDataKind::Uncalibrated
      raise ArgumentError.new("uncalibrated signal format is '#{signal_format.uncalibrated}', not int16") unless signal_format.uncalibrated_int16?
      signal(Int16)
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
end
