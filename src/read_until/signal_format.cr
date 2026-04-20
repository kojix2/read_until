module ReadUntil
  class SignalFormat
    getter calibrated : String
    getter uncalibrated : String

    def self.from_data_types(types : MinknowApi::Data::GetDataTypesResponse) : SignalFormat
      calibrated = dtype_name(types.calibrated_signal)
      uncalibrated = dtype_name(types.uncalibrated_signal)
      new(calibrated: calibrated, uncalibrated: uncalibrated)
    end

    def self.dtype_name(data_type : MinknowApi::Data::GetDataTypesResponse::DataType?) : String
      return "unknown" unless data_type

      signed = data_type.type_.raw == MinknowApi::Data::GetDataTypesResponse::DataType::Type::SIGNED_INTEGER.value
      unsigned = data_type.type_.raw == MinknowApi::Data::GetDataTypesResponse::DataType::Type::UNSIGNED_INTEGER.value
      floating = data_type.type_.raw == MinknowApi::Data::GetDataTypesResponse::DataType::Type::FLOATING_POINT.value

      base = if floating
               "float#{data_type.size * 8}"
             elsif signed
               "int#{data_type.size * 8}"
             elsif unsigned
               "uint#{data_type.size * 8}"
             else
               "unknown"
             end

      return base unless data_type.big_endian
      "#{base}_be"
    end

    def initialize(@calibrated : String = "float32", @uncalibrated : String = "int16")
    end

    def calibrated_float32? : Bool
      @calibrated == "float32"
    end

    def uncalibrated_int16? : Bool
      @uncalibrated == "int16"
    end

    def dtype_for(kind : RawDataKind) : String
      case kind
      when RawDataKind::Calibrated   then @calibrated
      when RawDataKind::Uncalibrated then @uncalibrated
      else                                "none"
      end
    end
  end
end
