#!/usr/bin/ruby

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
include Mongo

mongo_client = MongoClient.new("localhost", 27017)
files_coll = mongo_client.db("gsu")["files"]
devices_coll = mongo_client.db("gsu")["devices"]
data_coll = mongo_client.db("gsu")["data"]

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


begin

	probes = data_coll.distinct('probe')

	probes.each do |probe|

		if probe == "edu.mit.media.funf.probe.builtin.RunningApplicationsProbe" 
			#next
		end
		puts "====================== #{probe}"

		rows = data_coll.find('probe' => probe)

		puts "Collecting Keys..."

		keys = ["serial_id", "timestamp"]
		rows.each do |row|
			if row['value'] != nil 
				#puts row['value']
				value = JSON.parse(row['value'])
				# if value['RUNNING_TASKS'] != nil
				# 	task_index = 0
				# 	value['RUNNING_TASKS'].each do |task| 
				# 		task = make_hash_one_dimensional(task)
				# 		task.each do |key, v|
				# 			keys.concat([key]) 		
				# 		end
				# 		task_index += 1
				# 	end
				# 	value.delete('RUNNING_TASKS')		
				# end
				keys = keys.concat(value.keys).uniq
			end

		end
		keys.delete("TIMESTAMP")
		keys.delete("PROBE")
		if probe == "edu.mit.media.funf.probe.builtin.RunningApplicationsProbe"

			keys = keys.concat(['stack_id']).uniq
		end


		keys.map! {|key| key.downcase }
		puts keys.join(",")
		rows = data_coll.find('probe' => probe)

		rows.each do |row|
			newHash = Hash.new

			newHash['value'] = JSON.parse(row['value'])
			newHash['timestamp'] = row['timestamp']
			newHash['serial_id'] = row['serial_id'] 

			if row['probe'] == "edu.mit.media.funf.probe.builtin.RunningApplicationsProbe"
				newHash['stack_id'] = row['stack_id']
			end
			# if newHash['value']['RUNNING_TASKS'] != nil
			# 	task_index = 0
			# 	newHash['value']['RUNNING_TASKS'].each do |task| 
			# 		task = make_hash_one_dimensional(task)
			# 		task.each do |key, value|
			# 			newHash[key + "_" + task_index.to_s] = value
			# 		end
			# 		task_index += 1
			# 	end
			# 	newHash['value'].delete('RUNNING_TASKS')
			# end

			
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
			keys.each do |key|
				newValues.concat([newHash[key]])
			end			
			# newHash.each do |key, value| 
			# 	puts "\t #{key} : #{value}"
			# end
			puts newValues.join(",")
		end
		#puts keys.join(",")
	end

end	

