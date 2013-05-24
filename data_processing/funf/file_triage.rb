#!/usr/bin/ruby


##############################################################################
### file_triage.rb by [David Nunez](www.davidnunez.com), 2013
###
### Copyright 2013, David Nunez, MIT License
##############################################################################

require "rubygems"
require "bundler/setup"

require 'csv'
require 'sqlite3'
require 'parseconfig'
config = ParseConfig.new('main.config')
UPLOAD_PATH = config['UPLOAD_PATH']
DATA_RAW_PATH = config['DATA_RAW_PATH']
MERGED_DATABASE_NAME = "merged_#{Time.now.to_i}.db"

Dir.chdir UPLOAD_PATH

# 1367897932-00000258_037c7049408021d7_9aeaa421-044e-4a48-b229-774f764f0735_1367894345_mainPipeline.db

dir_contents = Dir["*.db"]
dir_contents.reject {|f| f.start_with? "merged"}
dir_contents.first(10000).each do |file|
	f = file.split('_')

	puts "Processing #{f[0]}" 
	upload_time = f[0].split('-')[0];
	sequential_id = f[0].split('-')[1];
	serial_id = f[1];
	collected_time = f[3]; 

	puts "#{serial_id}:#{sequential_id} Collected at #{collected_time}, Uploaded at: #{upload_time}"
end