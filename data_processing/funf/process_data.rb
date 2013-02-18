##############################################################################
### process_data.rb by [David Nunez](www.davidnunez.com), 2013
###
### Copyright 2013, David Nunez, MIT License
##############################################################################

#!/usr/bin/ruby
require "rubygems"
require "bundler/setup"

require 'csv'
require 'sqlite3'
require 'parseconfig'
config = ParseConfig.new('main.config')
UPLOAD_PATH = config['UPLOAD_PATH']
DATA_RAW_PATH = config['DATA_RAW_PATH']
MERGED_DATABASE_NAME = "merged_#{Time.now.to_i}.db"

Dir.chdir DATA_RAW_PATH

error_file = File.open("ERROR_LOG.txt", 'a')
processed_file = File.open("PROCESSED_FILE.txt", 'a')

puts "Syncing latest data..."
system "rsync -azv #{UPLOAD_PATH} #{DATA_RAW_PATH}"


merged_db = SQLite3::Database.new( MERGED_DATABASE_NAME )
rows = merged_db.execute <<-SQL
	SELECT name FROM sqlite_master WHERE type='table' AND name='data';
SQL
if rows.size == 0 
	p "Creating Merged Database Data Table"
	rows = merged_db.execute <<-SQL
		CREATE TABLE data (id text, device text, probe text, timestamp long, value text);
	SQL

	merged_db.execute <<-SQL
			CREATE TABLE master_id (serial_id text, device_id text, label text, version text);
	SQL
end


# Process each .db file

dir_contents = Dir["*.db"]
dir_contents.first(100).each do |f| 
	begin
		puts "Decrypting: " + f
		system "~/data_processing/bin/dbdecrypt.py -p 'changeme' #{f}"
			
	 	db = SQLite3::Database.new( f )
	  	file_info = db.get_first_row( "select * from file_info" )
	  	file_id, database_name, device_id, uuid, created = file_info

		filename_metadata = f[11, f.length].split('_')
		if filename_metadata.length < 4 then
			raise "Invalid filename"
		end
		serial_id = filename_metadata[1]

		puts "Mapping: #{device_id} to #{serial_id}"

		rows = merged_db.execute("SELECT serial_id FROM master_id WHERE serial_id=? and device_id=?",[serial_id, device_id])

		if rows.size == 0
			merged_db.execute("INSERT INTO master_id (serial_id, device_id, label, version) 
	            VALUES (?, ?, ?, ?)", [serial_id, device_id, "", ""])
			puts "Added mapping."
		else
			puts "Mapping already added."
		end
		
		rows = db.execute("select * from data") 
		rows.each do |row|
			data_id, probe, timestamp, value = row
			merged_id = uuid + '-' + data_id.to_s

			merged_db.execute("INSERT INTO data (id, device, probe, timestamp, value) 
	            VALUES (?, ?, ?, ?, ?)", [merged_id, device_id, probe, timestamp, value])

		end
		processed_file.puts(f)
	rescue => detail
		error_file.puts("ERROR: " + f + ': '+ detail)
		puts ("ERROR--------------------------------!")
	ensure
		db.close
	end
end


