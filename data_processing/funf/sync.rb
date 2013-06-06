#! /usr/bin/env ruby

##############################################################################
### master_id.rb by [David Nunez](www.davidnunez.com), 2013
###
### Copyright 2013, David Nunez, MIT License
##############################################################################

require "rubygems"
require "bundler/setup"

require 'csv'
require 'sqlite3'
require 'parseconfig'
require 'fileutils'

config = ParseConfig.new('main.config')
DATA_RAW_PATH = config['DATA_RAW_PATH']
DATA_RAW_CSV_PATH = config['DATA_RAW_CSV_PATH']
DATA_RAW_MERGED_DB_PATH = config['DATA_RAW_MERGED_DB_PATH']
DATA_PROCESSED_CSV_PATH = config['DATA_PROCESSED_CSV_PATH']
DATA_PROCESSED_PATH = config['DATA_PROCESSED_PATH']

Dir.chdir DATA_RAW_PATH

if !(File.exists? DATA_RAW_CSV_PATH)
	Dir.mkdir DATA_RAW_CSV_PATH
end

if !(File.exists? DATA_RAW_MERGED_DB_PATH)
	Dir.mkdir DATA_RAW_MERGED_DB_PATH
end

dir_contents = Dir["merged*.db"]
puts "Starting Sync..."

dir_contents.each do |f| 
	begin
		puts "Converting Merged DB #{f} to CSV..."
		system "~/data_processing/bin/db2csv.py #{f}"

		FileUtils.mv("#{DATA_RAW_PATH}/#{f}", "#{DATA_RAW_MERGED_DB_PATH}/#{f}")
		# Todo: append each csv to master csv files 

		dir_csv = Dir['*.csv']
		dir_csv.each do |c|
			begin
				puts "Appending #{c}... to #{DATA_RAW_CSV_PATH}/#{c}"

				master_csv_file = File.open("#{DATA_RAW_CSV_PATH}/#{c}", "a")
				csv_lines = File.readlines("#{c}")
				csv_lines[1..csv_lines.length].each do |l|
					master_csv_file.write(l)
				end
				system "rm #{c}"
			ensure
				master_csv_file.close
			end
		end


	rescue => detail
		puts ("ERROR--------------------------------")
		puts ("\t" + f + ': ' + detail)
		puts ("-------------------------------------")

	ensure

	end
end

puts "Syncing csv and wav files..."
system "rsync -azv #{DATA_RAW_CSV_PATH} #{DATA_PROCESSED_PATH}"
system "rsync -azv *.wav #{DATA_PROCESSED_PATH}"

