#! /usr/bin/env ruby

##############################################################################
### db_export_files_sum.rb by [David Nunez](www.davidnunez.com), 2013
###
### Copyright 2013, David Nunez, MIT License
##############################################################################

require "rubygems"
require "bundler/setup"

require 'csv'
require 'sqlite3'
require 'parseconfig'
require 'mongo'
require 'json'
require 'mongoid'
require 'chronic'
require 'date'
require './data_file.rb'
require './device'
include Mongo

Mongoid.load!("./mongoid.yml", :development)

## example document from data_files

#  {  "_id"             : ObjectId("51b733fa9fe64edd57000023"), 
#     "filename"        : "1347469758-00000071_17006189437fa597_051ae176-5498-43bc-b463-1bad504e3c1e_1347468796_mainPipeline.db", 
#     "ordinal_value"   : 71, 
#     "size"            : 48128, 
#     "processed"       : false, 
#     "collected_date"  : ISODate("2012-09-12T16:53:16Z"), 
#     "upload_date"     : ISODate("2012-09-12T17:09:18Z"), 
#     "device_id"       : ObjectId("51b733fa9fe64edd57000022") }

map = %Q{
  function() {
    day = Date.UTC(this.collected_date.getFullYear(), this.collected_date.getMonth(), this.collected_date.getDate());

    emit({day: day, device_id: this.device_id}, {sum: this.size});
  }
}

# Example Output from map:
# 
# ....
# {"_id"=>{"day"=>1370217600000.0, "device_id"=>"51b733f89fe64edd5700000b"}, "value"=>{"sum"=>2314.0}}
# {"_id"=>{"day"=>1370217600000.0, "device_id"=>"51b733f89fe64edd5700000b"}, "value"=>{"sum"=>12344.0}}
# {"_id"=>{"day"=>1370217500000.0, "device_id"=>"51b733f89fe64edd5700000b"}, "value"=>{"sum"=>3443.0}}
# {"_id"=>{"day"=>1370217300000.0, "device_id"=>"51b733f89fe64edd5700000b"}, "value"=>{"sum"=>51968.0}}
# ....

reduce = %Q{ 
  function(key, values) {
    var sum = 0;

    values.forEach(function(v) {
      sum += v['sum'];
    });

    return {sum: sum};
  }
}
# Example Output from reduce:
# ....
# {"_id"=>{"day"=>1370217600000.0, "device_id"=>"51b733f89fe64edd5700000b"}, "value"=>{"sum"=>51313968.0}}
# ....


# p DataFile.count
# p DataFile.map_reduce(map, reduce).out(inline: 1).emitted


DataFile.map_reduce(map, reduce).out(inline: 1).each do |document|
  # p document
  device = Device.find(document['_id']['device_id'])
  device_label = device.label
  if device_label == nil
    device_label = device.serial_id
  end
  t = Time.at(document['_id']['day']/1000).strftime("%Y-%m-%d")
  puts "#{device_label}" + "\t" + "#{t}" + "\t" + "#{document['value']['sum']}"
end
