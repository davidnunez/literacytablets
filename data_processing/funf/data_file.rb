class DataFile
  include Mongoid::Document

  field :filename, type: String
  field :ordinal_value, type: Integer
  field :serial_id, type: String
  field :size, type: Integer
  field :processed, type: Boolean
  field :collected_date, type: Time
  field :upload_date, type: Time

	def self.time_at_ms(ms)
		Time.at(ms / 1000, (ms % 1000) * 1000)
	end 
end