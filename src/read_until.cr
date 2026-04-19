require "minknow"

module ReadUntil
  VERSION = "0.1.0"

  enum ActionKind
    Unblock
    StopReceiving
  end

  record StreamSetup,
    first_channel : Int32 = 1,
    last_channel : Int32 = 512,
    one_chunk : Bool = false

  record ReadChunk,
    channel : Int32,
    read_id : String,
    number : Int32 = 0,
    raw_data : Bytes = Bytes.empty

  record Action,
    kind : ActionKind,
    channel : Int32,
    read_id : String

  class ReadUntilClient
    getter connection : Minknow::Connection?
    getter setup : StreamSetup

    def initialize(
      @connection : Minknow::Connection? = nil,
      @setup : StreamSetup = StreamSetup.new,
    )
      @running = false
      @pending_actions = [] of Action
      @latest_reads = {} of Int32 => ReadChunk
    end

    def endpoint : String?
      connection.try(&.endpoint)
    end

    def start : Nil
      @running = true
    end

    def stop : Nil
      @running = false
    end

    def running? : Bool
      @running
    end

    def ingest(chunk : ReadChunk) : ReadChunk
      @latest_reads[chunk.channel] = chunk
      chunk
    end

    def latest_read(channel : Int32) : ReadChunk?
      @latest_reads[channel]?
    end

    def enqueue_unblock(channel : Int32, read_id : String) : Action
      enqueue(Action.new(kind: ActionKind::Unblock, channel: channel, read_id: read_id))
    end

    def enqueue_stop_receiving(channel : Int32, read_id : String) : Action
      enqueue(Action.new(kind: ActionKind::StopReceiving, channel: channel, read_id: read_id))
    end

    def drain_actions : Array(Action)
      drained = @pending_actions
      @pending_actions = [] of Action
      drained
    end

    private def enqueue(action : Action) : Action
      @pending_actions << action
      action
    end
  end

  Client = ReadUntilClient
end
