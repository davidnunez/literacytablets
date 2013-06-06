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

include Mongo

Mongoid.load!("./mongoid.yml", :development)

puts DataFile.first.filename