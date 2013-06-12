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

map = %Q{
  function() {
  day = Date.UTC(this.collected_date.getFullYear(), this.collected_date.getMonth(), this.collected_date.getDate());

  emit({day: day, device_id: this.device_id}, {sum: this.size});
  }
}

reduce = %Q{ 
  function(key, values) {
  var sum = 0;

  values.forEach(function(v) {
    sum += v['sum'];
  });

  return {sum: sum};
  }
}

DataFile.map_reduce(map, reduce).out(inline: 1).each do |document|
  device = Device.find(document['_id']['device_id'])
  device_label = device.label
  if device_label == nil
    device_label = device.serial_id
  end
  t = Time.at(document['_id']['day']/1000).strftime("%Y-%m-%d")
  puts "#{device_label}" + "\t" + "#{t}" + "\t" + "#{document['value']['sum']}"

end
