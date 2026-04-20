module ReadUntil
  class Prefilter
    getter? enabled : Bool
    getter class_ids : Set(Int32)

    def self.none : Prefilter
      new(false, Set(Int32).new)
    end

    def self.strand_like(*classes : Symbol | ReadClass, registry : ClassificationRegistry = ClassificationRegistry.default) : Prefilter
      class_ids = Set(Int32).new
      classes.each do |klass|
        id = case klass
             when Symbol
               registry.id_for(klass)
             when ReadClass
               registry.id_for(klass)
             end

        unless id
          raise ArgumentError.new("unknown read class: #{klass}")
        end
        class_ids.add(id)
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
end
