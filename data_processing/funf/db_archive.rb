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

Dir.chdir DATA_RAW_PATH

RUN_TIME = Time.now.to_i
error_file = File.open("#{RUN_TIME}_ERROR_ARCHIVE_FILE.txt", 'a')
error_log_file = File.open("#{RUN_TIME}_ERROR_ARCHIVE_LOG.txt", 'a')




dir_contents = Dir["*.db"]
progress_index = 0
dir_contents.each do |f| 
	begin
		progress_index += 1
		if (progress_index % 1000 == 0)
			puts progress_index 
		end

		data_file = DataFile.where(filename: f, processed: true).first
		if data_file != nil
			directory = DATA_ARCHIVE_PATH + "/" + data_file.collected_date.strftime("%Y-%m")
			Dir.mkdir(directory) unless File.exists?(directory)
			FileUtils.mv(f, directory + "/" + f)
		end

	rescue => detail
		#error_log_file.puts("ERROR: " + f + ': '+ detail)
		error_msg = "ERROR--------------------------------\n" +
			"\t" + f + ': ' + detail.to_s + "\n" +
			"-------------------------------------"
		puts error_msg
		error_log_file.puts(error_msg)
		error_file.puts(f)

		#error_file.puts error_msg
	end
end