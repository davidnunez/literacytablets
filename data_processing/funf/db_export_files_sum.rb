#! /usr/bin/env ruby

##############################################################################
### db_export_files_sum.rb by [David Nunez](www.davidnunez.com), 2013
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
include Mongo

Mongoid.load!("./mongoid.yml", :development)

# example document from devices
# 
# {  "_id"        : "51b733f89fe64edd57000001", 
#    "label"      : "GSU-35"
#    "device_ids" : ["9aeaa421-044e-4a48-b229-774f764f0735"], 
#    "serial_id   : "037c7049408021d7" }

# Example 1: Count devices


# Example 2: Iterate over each device printing its label and id

Device.each do |device|
 # p "#{device.label}"
end

## example document from data_files

#  {  "_id"             : ObjectId("51b733fa9fe64edd57000023"), 
#     "filename"        : "1347469758-00000071_17006189437fa597_051ae176-5498-43bc-b463-1bad504e3c1e_1347468796_mainPipeline.db", 
#     "ordinal_value"   : 71, 
#     "size"            : 48128, 
#     "processed"       : false, 
#     "collected_date"  : ISODate("2012-09-12T16:53:16Z"), 
#     "upload_date"     : ISODate("2012-09-12T17:09:18Z"), 
#     "device_id"       : ObjectId("51b733fa9fe64edd57000022") }


# Example 3. Count data files

p DataFile.count

# Example 4. Get sum of all data_file sizes using DataFile.sum('size')

p DataFile.sum('size')

# Example 5. Find data_file count and sum of size per device. Iterate over each device and execute a query over DataFile using DataFile.where(:device => device).count and .sum('size')

Device.each do |device|
  p  DataFile.where(:device => device).count
end












# Device.each do | device | 
#  sum = DataFile.where(:device => device).sum('size')
#   p "#{device.label} : #{sum}"
# end



