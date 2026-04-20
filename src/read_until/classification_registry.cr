module ReadUntil
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

    CLASS_TO_SYMBOL = {
      ReadClass::Strand       => :strand,
      ReadClass::Strand1      => :strand1,
      ReadClass::Multiple     => :multiple,
      ReadClass::Zero         => :zero,
      ReadClass::Adapter      => :adapter,
      ReadClass::MuxUncertain => :mux_uncertain,
      ReadClass::User2        => :user2,
      ReadClass::User1        => :user1,
      ReadClass::Event        => :event,
      ReadClass::Pore         => :pore,
      ReadClass::Unavailable  => :unavailable,
      ReadClass::Transition   => :transition,
      ReadClass::Unclassed    => :unclassed,
    }

    getter ids_to_names : Hash(Int32, Symbol)
    getter names_to_ids : Hash(Symbol, Int32)

    @@default : ClassificationRegistry?
    @@default_mutex = Mutex.new

    def self.default : ClassificationRegistry
      @@default_mutex.synchronize do
        @@default ||= new
      end
    end

    def initialize(ids_to_names : Hash(Int32, Symbol) = DEFAULT_IDS_TO_NAMES.dup)
      @ids_to_names = ids_to_names
      @names_to_ids = {} of Symbol => Int32
      @ids_to_names.each { |id, name| @names_to_ids[name] = id }
    end

    def id_for(name : Symbol) : Int32?
      @names_to_ids[name]?
    end

    def id_for(klass : ReadClass) : Int32?
      symbol = CLASS_TO_SYMBOL[klass]?
      return nil unless symbol
      id_for(symbol)
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
end
