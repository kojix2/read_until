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
end
