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

mongo_client = MongoClient.new("localhost", 27017)
files_coll = mongo_client.db("gsu").db["files"]
devices_coll = mongo_client.db("gsu").db["devices"]

dir_contents = Dir["*.db"]

dir_contents.first(10000).each do |f| 
	begin
		puts "Decrypting: " + f
		system "~/data_processing/bin/dbdecrypt.py -p 'changeme' #{f}"
			
	 	db = SQLite3::Database.new( f )
	  	file_info = db.get_first_row( "select * from file_info" )
	  	file_id, database_name, device_id, uuid, created = file_info

	  	# Example Filename
	  	# 1362428241-00000174_0a7442444300a557_9385a79e-2d38-4917-8b67-8e600d1ba226_1358991911_mainPipeline.db

		filename_metadata = f.split('_')
		if filename_metadata.length != 5 then
			raise "Invalid filename"
		end

		upload_date = filename_metadata[0].splite('-')[0] 
		ordinal_value = filename_metadata[0].split('-')[1]
		serial_id = filename_metadata[1]
		device_id = filename_metadata[2]
		collected_date = filename_metadata[3]

		puts "Mapping: #{device_id} to #{serial_id}"

		if (devices_coll.find("device_id" => device_id).to_a.length == 0) 
			doc = {"serial_id" => serial_id, "device_id" => device_id}
			devices_coll.insert(doc)
		end

		puts "Storing File"

		doc = {"filename" => f,
				"upload_date" => upload_date, 
				"ordinal_value" => ordinal_value, 
				"serial_id" => serial_id, 
				"collected_date" => collected_date}

		files_coll.insert(doc);


		# rows = merged_db.execute("SELECT serial_id FROM master_id WHERE serial_id=? and device_id=?",[serial_id, device_id])

		# if rows.size == 0
		# 	merged_db.execute("INSERT INTO master_id (serial_id, device_id, label, version) 
	 #            VALUES (?, ?, ?, ?)", [serial_id, device_id, "", ""])
		# 	puts "Added mapping."
		# else
		# 	puts "Mapping already added."
		# end
		
		# rows = db.execute("select * from data") 
		# rows.each do |row|
		# 	data_id, probe, timestamp, value = row
		# 	merged_id = uuid + '-' + data_id.to_s

		# 	merged_db.execute("INSERT INTO data (id, device, probe, timestamp, value) 
	 #            VALUES (?, ?, ?, ?, ?)", [merged_id, device_id, probe, timestamp, value])

		# end
		# processed_file.puts(f)
	rescue => detail
		#error_log_file.puts("ERROR: " + f + ': '+ detail)
		puts ("ERROR--------------------------------")
		puts ("\t" + f + ': ' + detail)
		puts ("-------------------------------------")
		#error_file.puts(f)

	ensure
		db.close
	end
end
