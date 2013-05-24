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

files_coll.remove
devices_coll.remove
data_coll.remove


dir_contents = Dir["*.db"]

dir_contents.first(1000).each do |f| 
	begin

			
	 	db = SQLite3::Database.new( f )
		if files_coll.find('filename' => f).to_a.length != 0
			next
		end

		puts "Processing: " + f
		system "~/data_processing/bin/dbdecrypt.py -p 'changeme' #{f}"

	  	file_info = db.get_first_row( "select * from file_info" )
	  	file_id, database_name, device_id, uuid, created = file_info



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

		puts "Mapping: #{device_id} to #{serial_id}"

		if (devices_coll.find("device_id" => device_id).to_a.length == 0) 
			doc = {"serial_id" => serial_id, "device_id" => device_id}
			devices_coll.insert(doc)
		end

		puts "Storing File Metadata"

		doc = {"filename" => f,
				"upload_date" => upload_date, 
				"ordinal_value" => ordinal_value, 
				"serial_id" => serial_id, 
				"collected_date" => collected_date,
				"processed" => false}
		files_coll.insert(doc);



		puts "Storing Data"
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

				j = JSON.parse(doc['value'])
				data_coll.insert(doc)
			
			rescue => detail
				error_msg = "ERROR--------------------------------\n" +
					"\t" + doc['probe'] + ': ' + detail + "\n" +
					"\t" + doc.inspect + "\n" +
					"-------------------------------------"
				puts error_msg
				#error_file.puts error_msg
			end


		end
		files_coll.update({"filename" => f}, {"$set" => {"processed" => true}})

		# processed_file.puts(f)
	rescue => detail
		#error_log_file.puts("ERROR: " + f + ': '+ detail)
		error_msg = "ERROR--------------------------------\n" +
			"\t" + f + ': ' + detail + "\n" +
			"-------------------------------------"
		puts error_msg
		#error_file.puts error_msg
	ensure
		db.close
	end

end
