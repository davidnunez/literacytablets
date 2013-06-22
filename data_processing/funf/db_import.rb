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
require './data_file.rb'
require './device.rb'

require 'parseconfig'
include Mongo
Mongoid.load!("mongoid.yml", :development)


config = ParseConfig.new('main.config')
DATA_RAW_PATH = config['DATA_RAW_PATH']



Dir.chdir DATA_RAW_PATH




mongo_client = MongoClient.new(config['MONGO_HOST'], config['MONGO_PORT'])
data_coll = mongo_client.db(config['MONGO_DB'])["data"]


RUN_TIME = Time.now.to_i


error_file = File.open("./logs/#{RUN_TIME}_ERROR_FILE.txt", 'a')
error_log_file = File.open("./logs/#{RUN_TIME}_ERROR_LOG.txt", 'a')

#data_coll.remove
#Device.delete_all
#DataFile.delete_all

class BSON::OrderedHash
  def to_h
    inject({}) { |acc, element| k,v = element; acc[k] = (if v.class == BSON::OrderedHash then v.to_h else v end); acc }
  end

  def to_json
    to_h.to_json
  end
end


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

dir_contents = Dir["*.db"]
progress_index = 0
dir_contents.each do |f| 
	begin
		progress_index += 1
		if (progress_index % 1000 == 0)
			puts progress_index 
		end
		if DataFile.where(filename: f).exists?
			# puts "Already seen #{f} - skipping"
			next
		end

		# puts "Processing: " + f
		system "~/data_processing/bin/dbdecrypt.py -p 'changeme' #{f}"




	  	# Example Filename
	  	# 1362428241-00000174_0a7442444300a557_9385a79e-2d38-4917-8b67-8e600d1ba226_1358991911_mainPipeline.db

		filename_metadata = f.split('_')
		if filename_metadata.length != 5 then
			raise "Invalid filename"
		end

		upload_date = filename_metadata[0].split('-')[0] 
		ordinal_value = filename_metadata[0].split('-')[1]
		serial_id = filename_metadata[1]
		device_id = filename_metadata[2]
		collected_date = filename_metadata[3]


			if (upload_date.to_s.length == 10) 
				upload_date = upload_date.to_i * 1000
			end
			if (collected_date.to_s.length == 10) 
				collected_date = collected_date.to_i * 1000
			end

		# puts "Mapping: #{device_id} to #{serial_id}"

		device = Device.where(serial_id: serial_id).first
		if (device == nil)
			raise "Could not find Device with serial #{serial_id}"
		end

		if device.device_ids == nil
			device.device_ids = []
		end
		device.device_ids = (device.device_ids << device_id).uniq
		device.save

		# puts "Storing File Metadata"


		df = DataFile.create!(
			filename: f,
			ordinal_value: ordinal_value,
			size: File.new(f).stat.size,
			processed: false,
			collected_date: DataFile.time_at_ms(collected_date),
			upload_date: DataFile.time_at_ms(upload_date),
			device: device
		)

		# puts "Storing Data"
	 	db = SQLite3::Database.new( f )
	  	file_info = db.get_first_row( "select * from file_info" )
	  	file_id, database_name, device_id, uuid, created = file_info

		rows = db.execute("select * from data") 
		rows.each do |row|
			data_id, probe, timestamp, value = row

			if (timestamp.to_s.length == 10) 
				timestamp *= 1000
			end

			merged_id = uuid + '-' + data_id.to_s
			doc = {"data_id" => merged_id,
					"serial_id" => serial_id,
					"probe" => probe,
					"timestamp" => timestamp,
					"value" => value }
			begin 
#				puts doc["value"]
				## ANECDOTAL FIXES

				if doc['probe'] == "LauncherApp" 
					newValue = '{"app" : "' + doc["value"] + '"}'
					doc["value"] = newValue
				end
				if doc['probe'] == "FileMoverService" 
					doc["value"] += "}"
				end
				if doc['probe'] == "edu.mit.media.funf.probe.builtin.RunningApplicationsProbe"

 					value = JSON.parse(doc['value'])
 					if value['RUNNING_TASKS'] != nil
 						task_index = 0
 						value['RUNNING_TASKS'].each do |task| 
 							task = make_hash_one_dimensional(task)
 							d = {"data_id" => merged_id,
								"serial_id" => serial_id,
								"probe" => probe,
								"timestamp" => timestamp,
								"stack_id" => task_index};
							d = d.merge({"value" => task.to_json})
							data_coll.insert(d)
							task_index += 1
 						end
 						value.delete('RUNNING_TASKS')		
					end
					next
				end


				j = JSON.parse(doc['value'])
				data_coll.insert(doc)
			
			rescue => detail
				error_msg = "ERROR--------------------------------\n" +
					"\t" + doc['probe'] + ': ' + detail + "\n" +
					"\t" + doc.inspect + "\n" +
					"-------------------------------------"
				puts error_msg
				#error_file.puts error_msg
			
				error_log_file.puts(error_msg)
				error_file.puts(f)


			end


		end

		DataFile.where(filename: f).update(processed: true)
		# processed_file.puts(f)
	rescue => detail
		#error_log_file.puts("ERROR: " + f + ': '+ detail)
		data_file = DataFile.where(filename: f).first_or_create.update(processed: false)
		error_msg = "ERROR--------------------------------\n" +
			"\t" + f + ': ' + detail.to_s + "\n" +
			"-------------------------------------"
		puts error_msg
		error_log_file.puts(error_msg)
		error_file.puts(f)
		#error_file.puts error_msg
	ensure
		if (db != nil)
			db.close
		end
	end

end

