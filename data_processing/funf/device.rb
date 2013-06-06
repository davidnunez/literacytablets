require './data_file.rb'


class Device
  include Mongoid::Document

  field :label, type: String
  field :device_id, type: String
  field :serial_id, type: String
  has_many :DataFiles

end