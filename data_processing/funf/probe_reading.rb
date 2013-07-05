class ProbeReading
  include Mongoid::Document

  field :data_id, type: String
  field :serial_id, type: String
  field :probe, type: String
  field :timestamp, type: Integer
  field :value, type: String
  field :stack_id, type: Integer # HACK FOR "edu.mit.media.funf.probe.builtin.RunningApplicationsProbe"

	def self.time_at_ms(ms)
		Time.at(ms / 1000, (ms % 1000) * 1000)
	end 
end