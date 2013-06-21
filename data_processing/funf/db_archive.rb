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
require 'fileutils'

require './data_file.rb'
require './device.rb'

require 'parseconfig'
include Mongo
Mongoid.load!("mongoid.yml", :development)


config = ParseConfig.new('main.config')

DATA_RAW_PATH = config['DATA_RAW_PATH']
DATA_ARCHIVE_PATH = config['DATA_ARCHIVE_PATH']
DATA_ERROR_PATH = config['DATA_ERROR_PATH']

Dir.chdir DATA_RAW_PATH

RUN_TIME = Time.now.to_i
ERROR_FILE = File.open("#{RUN_TIME}_ERROR_ARCHIVE_FILE.txt", 'a')
ERROR_LOG_FILE = File.open("#{RUN_TIME}_ERROR_ARCHIVE_LOG.txt", 'a')


def log_error(detail, f)
		error_msg = "ERROR--------------------------------\n" +
			"\t" + f + ': ' + detail.to_s + "\n" +
			"-------------------------------------"
		puts error_msg
		ERROR_LOG_FILE.puts(error_msg)
		ERROR_FILE.puts(f)
		FileUtils.mv(f, DATA_ERROR_PATH + "/" + f)

		
end


def archive_file(f, directory, progress_index)
	begin
		progress_index += 1
		if (progress_index % 1000 == 0)
			puts progress_index 
		end

		Dir.mkdir(directory) unless File.exists?(directory)
		FileUtils.mv(f, directory + "/" + f)

	rescue => detail
		log_error detail, f
	end

end

#---------------------------------------------------------------

puts "STARTING .DB ARCHIVE"

dir_contents = Dir["*.db"]
progress_index = 0
dir_contents.each do |f| 
	begin
		data_file = DataFile.where(filename: f).first
		if data_file != nil
			if data_file.processed
				directory = DATA_ARCHIVE_PATH + "/" + data_file.collected_date.strftime("%Y-%m")
				archive_file f, directory, progress_index
			else
				raise "In Database, but not processed"
			end
		end
	rescue => detail
		log_error detail, f
	end
end

puts "STARTING .WAV ARCHIVE"

dir_contents = Dir["*.wav"]
progress_index = 0
dir_contents.each do |f| 
	begin
		
		# Example Filename
	  	# 1366909541-0270308243606617_1365623457278_recording.wav

		filename_metadata = f.split('_')
		if filename_metadata.length != 3 then
			raise "Invalid filename"
		end

		collected_date = filename_metadata[1]

		if collected_date.length != 13 then
			raise "Invalid date"
		end



		directory = DATA_ARCHIVE_PATH + "/" + DataFile.time_at_ms(collected_date.to_i).strftime("%Y-%m")
		archive_file f, directory, progress_index

	rescue  => detail
		log_error detail, f
	end

end





