#! /usr/bin/env ruby

##############################################################################
### db_import.rb by [David Nunez](www.davidnunez.com), 2013
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
require './probe_reading.rb'
include Mongo

config = ParseConfig.new('main.config')
DATA_PROCESSED_CSV_PATH = config['DATA_PROCESSED_CSV_PATH']

Mongoid.load!("./mongoid.yml", :development)

def make_hash_one_dimensional(input = {}, output = {}, options = {})
  input.each do |key, value|
    key = options[:prefix].nil? ? "#{key}" : "#{options[:prefix]}#{options[:delimiter]||"_"}#{key}"
    if value.is_a? Hash
      make_hash_one_dimensional(value, output, :prefix => key, :delimiter => "_")
    else
      output[key]  = value
    end
  end
  output
end

probe_names = [
	"FileMoverService",
	"GpsDateFixService",
	"LauncherApp",
	"Matching",
	"edu.mit.media.funf.bgcollector.MainPipeline",
	"edu.mit.media.funf.probe.builtin.BatteryProbe",
	"edu.mit.media.funf.probe.builtin.HardwareInfoProbe",
	"edu.mit.media.funf.probe.builtin.RunningApplicationsProbe",
	"edu.mit.media.funf.probe.builtin.ScreenProbe",
	"tinkerbook"
]

probe_keys = {} 
puts "Gathering Keys..."
probe_names.each do |probe_name|
	probe_reading =	ProbeReading.where(probe: probe_name).first
	puts "====================== #{probe_reading.probe}"
	keys = ["label", "serial_id", "timestamp"]
	value = JSON.parse(probe_reading.value)
	keys = keys.concat(value.keys).uniq
	keys.delete("TIMESTAMP")
	keys.delete("PROBE")
	if probe_reading.probe == "edu.mit.media.funf.probe.builtin.RunningApplicationsProbe"
		keys = keys.concat(['stack_id']).uniq
	end


	keys.map! {|key| key.downcase }
	puts keys.join(",")
	probe_keys[probe_name] = keys
	begin
		File.delete(DATA_PROCESSED_CSV_PATH + "/" + probe_name + ".csv")
	rescue => detail
	end
	File.open(DATA_PROCESSED_CSV_PATH + "/" + probe_name + ".csv", "a") { |f| 
		f.puts keys.join(",")
	}

end

puts "Exporting Probe Readings... #{ProbeReading.count}"

begin
	progress_index = 0
	ProbeReading.each do |probe_reading|

		progress_index += 1
		if (progress_index % 1000 == 0)
			puts progress_index 
		end
		# (progress_index > 20000) ? (exit) : (progress_index)

		# puts "====================== #{probe_reading.probe}"
		# keys = ["serial_id", "timestamp"]
		# value = JSON.parse(probe_reading.value)
		# 	keys = keys.concat(value.keys).uniq
		# puts keys.join(",")
		# puts "---------------"
		# keys.delete("TIMESTAMP")
		# keys.delete("PROBE")
		# if probe_reading.probe == "edu.mit.media.funf.probe.builtin.RunningApplicationsProbe"
		# 	keys = keys.concat(['stack_id']).uniq
		# end


		# keys.map! {|key| key.downcase }
		# puts keys.join(",")


# --------------------

		newHash = Hash.new

		newHash['value'] = JSON.parse(probe_reading.value)
		newHash['timestamp'] = probe_reading.timestamp
		newHash['serial_id'] = probe_reading.serial_id

		if probe_reading.probe == "edu.mit.media.funf.probe.builtin.RunningApplicationsProbe"
			newHash['stack_id'] = probe_reading.stack_id
		end
		
		newHash = make_hash_one_dimensional(newHash)
		
		if newHash["timestamp"].to_i == newHash["value_TIMESTAMP"].to_i*1000
			newHash.delete("value_TIMESTAMP")
		end

		newHash.keys.each do |key|
			if key.match(/^value_/) 
			    newHash[key[6, key.length].downcase] = newHash.delete(key)
			end
		end
		newHash.delete('probe')

		newValues = []
		probe_keys[probe_reading.probe].each do |key|
			newValues.concat([newHash[key]])
		end			
		File.open(DATA_PROCESSED_CSV_PATH + "/" + probe_reading.probe + ".csv", "a") { |f| 
			f.puts Device.where(serial_id: probe_reading.serial_id).first.label + newValues.join(",")
		}
	end

end	

