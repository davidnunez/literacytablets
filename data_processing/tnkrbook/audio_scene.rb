#!/usr/bin/env ruby

##############################################################################
### audio_scene.rb by [David Nunez](www.davidnunez.com), 2013
### Parses tinkerbook generated .txt file and creates a mapping between 
### scene and audio information.
###
### - takes as arguments one or more text filenames (uses filenames as tablet
### id)
###
### Copyright 2013, David Nunez, MIT License
##############################################################################

require 'rubygems'
require 'json'

ARGV.each do|a|
  File.open(a, "r") do |file|
  	current_scene = ""
  	file.each_line do |line|
  		if line.include?("\"type\":\"recording\"")
  			j = line.split("\t")[1]
  			jp = JSON.parse(j)
  			puts '"' + a + '"' + ',' + '"' + current_scene + '"' + "," + '"' + jp["filename"] + '"'
  		end
  		if line.include?("\"type\":\"Scene\"") 
  			j = line.split("\t")[1]
  			jp = JSON.parse(j)
  			jpv = jp["values"]
  			current_scene = jpv["url"]
  		end
  	end
  end
end